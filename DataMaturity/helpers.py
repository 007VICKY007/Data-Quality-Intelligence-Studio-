import numpy as np
import pandas as pd
from io import BytesIO

import streamlit as st

from DataMaturity.config import (
    RATING_LABELS, RATING_TO_SCORE, DQ_MATURITY_MAP,
    DEFAULT_MASTER_OBJECTS, MATURITY_DIMS, QUESTION_BANK,
)
MATURITY_COLOR_MAP = {
    "Adhoc":      "EF4444",
    "Repeatable": "F59E0B",
    "Defined":    "38BDF8",
    "Managed":    "A855F7",
    "Optimised":  "10B981",
}
# ──────────────────────────────────────────────────────────────
# DQ Score  →  Maturity Level
# ──────────────────────────────────────────────────────────────
def dq_score_to_maturity_level(dq_score: float) -> str:
    """
    Convert a DQ engine percentage score (0–100) to a DAMA maturity label.

    Thresholds
    ----------
    >= 95  →  Optimised
    >= 80  →  Managed
    >= 60  →  Defined
    >= 40  →  Repeatable
     < 40  →  Adhoc
    """
    for threshold, level in DQ_MATURITY_MAP:
        if dq_score >= threshold:
            return level
    return "Adhoc"


# ──────────────────────────────────────────────────────────────
# Session State
# ──────────────────────────────────────────────────────────────
def init_maturity_state() -> None:
    """Initialise all session-state keys used by the Maturity module."""
    defaults = {
        "mat_client_name":  "",
        "mat_objects":      DEFAULT_MASTER_OBJECTS[:],
        "mat_dims":         MATURITY_DIMS[:],
        "mat_responses":    {},
        "mat_submitted":    False,
        "mat_payload":      {},
        "mat_benchmark":    3.0,
        "mat_target":       3.0,
        "mat_low_thr":      2.0,
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


# ──────────────────────────────────────────────────────────────
# Response Table Builders
# ──────────────────────────────────────────────────────────────
def build_question_df(dimension: str, objects: list) -> pd.DataFrame:
    """Return a blank response DataFrame for one maturity dimension."""
    qs = QUESTION_BANK[dimension]
    df = pd.DataFrame({
        "Question ID": [q["id"]            for q in qs],
        "Section":     [q["section"]        for q in qs],
        "Question":    [q["question"]       for q in qs],
        "Weight":      [q.get("weight", 1)  for q in qs],
    })
    for obj in objects:
        df[obj] = RATING_LABELS[0]
    return df


def sync_response_tables() -> None:
    """
    Ensure every selected dimension has a response table that matches
    the current master-object selection.  Adds missing object columns,
    removes stale ones, and creates missing dimension tables.
    """
    objects = st.session_state.mat_objects
    for dim in st.session_state.mat_dims:
        if dim not in st.session_state.mat_responses:
            st.session_state.mat_responses[dim] = build_question_df(dim, objects)

        df   = st.session_state.mat_responses[dim].copy()
        keep = ["Question ID", "Section", "Question", "Weight"] + objects

        for obj in objects:
            if obj not in df.columns:
                df[obj] = RATING_LABELS[0]

        for col in list(df.columns):
            if col not in keep:
                df = df.drop(columns=[col])

        st.session_state.mat_responses[dim] = df[keep]


def autofill_dq_dimension(dq_score: float) -> None:
    """
    Auto-populate every question in the *Data Quality* dimension with
    the maturity level that corresponds to the DQ engine score.
    Called right after a successful DQ run.
    """
    if "Data Quality" not in st.session_state.mat_dims:
        return

    level   = dq_score_to_maturity_level(dq_score)
    objects = st.session_state.mat_objects

    if "Data Quality" not in st.session_state.mat_responses:
        st.session_state.mat_responses["Data Quality"] = build_question_df("Data Quality", objects)

    df = st.session_state.mat_responses["Data Quality"]
    for obj in objects:
        if obj in df.columns:
            df[obj] = level

    st.session_state.mat_responses["Data Quality"] = df


# ──────────────────────────────────────────────────────────────
# Scoring
# ──────────────────────────────────────────────────────────────
def compute_weighted_scores(df: pd.DataFrame, objects: list) -> pd.DataFrame:
    """Replace rating strings with numeric scores (1-5) in a copy."""
    s = df.copy()
    for obj in objects:
        if obj in s.columns:  # Only process if column exists
            s[obj] = s[obj].map(RATING_TO_SCORE).astype(float)
    return s


def _dim_score_series(dim: str, df: pd.DataFrame, objects: list) -> pd.Series:
    """Weighted average score per object for one dimension."""
    s = compute_weighted_scores(df, objects)
    w = s["Weight"].astype(float).values
    row = {}
    for obj in objects:
        if obj in s.columns:  # Only process if column exists
            vals = s[obj].astype(float).values
            mask = np.isfinite(vals) & np.isfinite(w) & (w > 0)
            row[obj] = float(np.average(vals[mask], weights=w[mask])) if mask.sum() > 0 else np.nan
        else:
            row[obj] = np.nan
    return pd.Series(row, name=dim)


def compute_all_scores(
    objects: list,
    dims: list,
    responses: dict,
) -> tuple:
    """
    Compute all maturity scores.

    Returns
    -------
    dim_table : pd.DataFrame   (dims × objects, weighted-avg 1-5)
    overall   : pd.Series      (overall score per object)
    """
    dim_rows  = [_dim_score_series(dim, responses[dim], objects) for dim in dims]
    dim_table = pd.DataFrame(dim_rows)
    overall   = dim_table.mean(axis=0, numeric_only=True)
    overall.name = "Overall"
    return dim_table, overall


# ──────────────────────────────────────────────────────────────
# Validation
# ──────────────────────────────────────────────────────────────
def validate_responses(responses: dict, dims: list, objects: list) -> tuple:
    """Return (ok: bool, error_message: str)."""
    for dim in dims:
        df = responses[dim]
        for obj in objects:
            if obj not in df.columns:
                return False, f"Missing column '{obj}' in dimension '{dim}'."
            bad = df[obj][~df[obj].isin(RATING_LABELS)]
            if len(bad):
                return False, f"Invalid rating values found in {dim} / {obj}."
    return True, ""


# ──────────────────────────────────────────────────────────────
# Safe helpers
# ──────────────────────────────────────────────────────────────
def safe_float(v) -> float:
    """Best-effort cast to float; returns np.nan on failure."""
    try:
        return float(v)
    except Exception:
        return np.nan


def safe_rating(v, default: int = 0) -> int:
    """Convert a rating value to int in [0, 5]; returns default on failure."""
    fv = safe_float(v)
    return int(np.clip(round(fv), 0, 5)) if np.isfinite(fv) else default


# ──────────────────────────────────────────────────────────────
# Excel Export
# ──────────────────────────────────────────────────────────────
def to_excel_bytes(
    dim_table:     pd.DataFrame,
    overall:       pd.Series,
    detail_tables: dict,
    low_thr:       float = 2.0,
    objects:       list  = None,
) -> bytes:
    """
    Build an Excel workbook with:
      - Summary – Dimension Scores
      - Summary – Overall Scores
      - Detail sheet per dimension
      - Exception sheets (scores ≤ low_thr) per object per dimension
    """
    objects = objects or list(overall.index)
    out     = BytesIO()

    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        dim_table.to_excel(writer, sheet_name="Summary - Dimension Scores")
        pd.DataFrame(overall).to_excel(writer, sheet_name="Summary - Overall Scores")

        for dim, df in detail_tables.items():
            d = df.copy()
            d.insert(0, "Dimension", dim)
            d.to_excel(writer, sheet_name=f"Detail - {dim[:20]}", index=False)

        # Generate exception sheets only for objects that exist in the dataframes
        for dim, df in detail_tables.items():
            s = compute_weighted_scores(df, objects)
            
            # Get the list of objects that actually exist in this dimension's dataframe
            existing_objects = [obj for obj in objects if obj in s.columns]
            
            for obj in existing_objects:
                # Create filter for exceptions
                exc = s[s[obj] <= low_thr][
                    ["Question ID", "Section", "Question", "Weight", obj]
                ].copy()
                
                if len(exc) > 0:
                    # Create safe sheet name (max 31 chars)
                    sheet_name = f"Exc-{obj[:10]}-{dim[:8]}"[:31]
                    exc.to_excel(
                        writer,
                        sheet_name=sheet_name,
                        index=False,
                    )

    return out.getvalue()