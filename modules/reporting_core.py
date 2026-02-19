"""
modules/reporting_core.py
══════════════════════════════════════════════════════════════════════════
Merged module: scoring_engine + report_generator

Contains:
  • ScoringService        — overall / column / dimension DQ scores
  • ExcelReportGenerator  — multi-sheet Excel DQ workbook builder
══════════════════════════════════════════════════════════════════════════
"""

import json
import logging
import datetime
import pandas as pd
import numpy as np
from io import BytesIO
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════
#  SCORING SERVICE
# ══════════════════════════════════════════════════════════════════════════

class ScoringService:
    """Calculate DQ scores at overall / column / dimension level."""

    @staticmethod
    def calculate_overall_score(results_df: pd.DataFrame) -> float:
        total = len(results_df)
        if total == 0:
            return 0.0
        clean = len(results_df[results_df["Count of issues"] == 0])
        return round((clean / total) * 100, 2)

    @staticmethod
    def calculate_column_scores(
        results_df: pd.DataFrame,
        all_columns: List[str],
    ) -> Dict[str, float]:
        total = len(results_df)
        if total == 0:
            return {}

        failed_tracker: Dict[str, set] = defaultdict(set)
        for idx, row in results_df.iterrows():
            for col in (row.get("_failed_columns_list") or []):
                failed_tracker[col].add(idx)

        scores: Dict[str, float] = {}
        _skip = {"Issues", "Count of issues", "Failed_Rules", "Failed_Columns", "Issue categories"}
        for col in all_columns:
            if col.startswith("_") or col in _skip:
                continue
            failed_count = len(failed_tracker.get(col, set()))
            scores[col] = round(((total - failed_count) / total) * 100, 2)
        return scores

    @staticmethod
    def calculate_dimension_scores(results_df: pd.DataFrame) -> Dict[str, float]:
        total = len(results_df)
        if total == 0:
            return {}

        dimensions: set = set()
        for dims in results_df["Issue categories"].dropna():
            dimensions.update(d.strip() for d in str(dims).split(",") if d.strip())

        scores: Dict[str, float] = {}
        for dim in dimensions:
            failed = len(results_df[results_df["Issue categories"].str.contains(dim, na=False)])
            scores[dim] = round(((total - failed) / total) * 100, 2)
        return scores


# ══════════════════════════════════════════════════════════════════════════
#  HELPERS (shared)
# ══════════════════════════════════════════════════════════════════════════

def clean_value(val):
    """Clean and format cell values for Excel output."""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(val, (list, tuple, np.ndarray)):
        return ", ".join(str(v) for v in val)
    if isinstance(val, dict):
        return str(val)
    if isinstance(val, bool):
        return "Yes" if val else "No"
    if isinstance(val, (int, float)):
        return val
    str_val = str(val).strip()
    return "" if str_val.lower() == "nan" else str_val


def get_timestamp() -> str:
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


# ══════════════════════════════════════════════════════════════════════════
#  EXCEL REPORT GENERATOR
# ══════════════════════════════════════════════════════════════════════════

class ExcelReportGenerator:
    """Generate multi-sheet Excel DQ assessment reports."""

    def __init__(
        self,
        results_df: pd.DataFrame,
        rulebook: Dict,
        all_columns: List[str],
        column_scores: Dict[str, float],
        overall_score: float,
        dimension_scores: Dict[str, float],
        duplicate_combinations: Optional[Dict[str, List]] = None,
    ):
        self.results_df           = results_df
        self.rulebook             = rulebook
        self.all_columns          = all_columns
        self.column_scores        = column_scores
        self.overall_score        = overall_score
        self.dimension_scores     = dimension_scores
        self.duplicate_combinations = duplicate_combinations or {}

    # ── Public ────────────────────────────────────────────────────────────

    def generate_report(self, output_dir: Path) -> Path:
        """Build the complete Excel workbook and return its path."""
        output_path = Path(output_dir) / "DQ_Assessment_Report.xlsx"
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Clean internal columns from output copy
        internal = [c for c in ("_failed_columns_list", "_failed_rules_details")
                    if c in self.results_df.columns]
        df_out = self.results_df.drop(columns=internal, errors="ignore").copy()
        for col in df_out.columns:
            try:
                df_out[col] = df_out[col].apply(clean_value)
            except Exception:
                df_out[col] = df_out[col].astype(str)

        failed_tracker     = self._build_failed_tracker()
        dimension_tracker  = self._build_dimension_tracker()
        uniqueness_failures= self._build_uniqueness_failures()

        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            wb  = writer.book
            fmt = self._make_formats(wb)

            self._sheet_dq_score(writer, fmt)
            self._sheet_summary(writer, df_out, fmt, failed_tracker)
            self._sheet_results(writer, df_out, fmt)
            self._sheet_dimension(writer, fmt)
            self._sheet_duplicate_summary(writer, fmt)
            self._sheets_annexures(writer, df_out, fmt, failed_tracker)
            self._sheet_uniqueness(writer, df_out, fmt, uniqueness_failures)
            self._sheet_completeness(writer, df_out, fmt, dimension_tracker)
            self._sheet_standardization(writer, df_out, fmt, dimension_tracker)

        logger.info(f"Report generated: {output_path}")
        return output_path

    def save_rulebook_json(self, output_dir: Path, rulebook: Optional[Dict] = None) -> Path:
        """Save rulebook as JSON and return path."""
        rb = rulebook or self.rulebook
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        path = Path(output_dir) / f"Rulebook_{get_timestamp()}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rb, f, indent=2, default=str)
        return path

    # ── Trackers ──────────────────────────────────────────────────────────

    def _build_failed_tracker(self) -> Dict[str, set]:
        tracker: Dict[str, set] = defaultdict(set)
        for idx, row in self.results_df.iterrows():
            for col in (row.get("_failed_columns_list") or []):
                tracker[col].add(idx)
        return tracker

    def _build_dimension_tracker(self) -> Dict[str, Dict[str, set]]:
        tracker = defaultdict(lambda: defaultdict(set))
        for idx, row in self.results_df.iterrows():
            for rd in (row.get("_failed_rules_details") or []):
                if isinstance(rd, dict):
                    dim = rd.get("dimension", "General")
                    col = rd.get("column")
                    if col:
                        tracker[dim][col].add(idx)
        return tracker

    def _build_uniqueness_failures(self) -> Dict[str, List[int]]:
        failures: Dict[str, List[int]] = defaultdict(list)
        for idx, row in self.results_df.iterrows():
            for rd in (row.get("_failed_rules_details") or []):
                if isinstance(rd, dict) and rd.get("rule_type") == "uniqueness":
                    col = rd.get("column")
                    if col:
                        failures[col].append(idx)
        return failures

    # ── Formats ───────────────────────────────────────────────────────────

    @staticmethod
    def _make_formats(wb) -> Dict:
        return {
            "title":       wb.add_format({"bold": True, "font_size": 14, "bg_color": "#4472C4", "font_color": "white", "align": "center", "valign": "vcenter", "border": 1}),
            "subtitle":    wb.add_format({"bold": True, "font_size": 11, "bg_color": "#D9E1F2", "align": "left", "border": 1}),
            "header":      wb.add_format({"bold": True, "bg_color": "#4472C4", "font_color": "white", "border": 1, "align": "center", "valign": "vcenter"}),
            "pass":        wb.add_format({"bg_color": "#C6EFCE", "font_color": "#006100", "bold": True, "align": "center", "border": 1}),
            "fail":        wb.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006", "bold": True, "align": "center", "border": 1}),
            "warning":     wb.add_format({"bg_color": "#FFEB9C", "font_color": "#9C6500", "bold": True, "align": "center", "border": 1}),
            "data":        wb.add_format({"border": 1, "align": "left", "valign": "vcenter"}),
            "data_center": wb.add_format({"border": 1, "align": "center", "valign": "vcenter"}),
            "metric":      wb.add_format({"bold": True, "align": "right", "border": 1}),
            "percentage":  wb.add_format({"num_format": "0.00%", "align": "right", "border": 1}),
        }

    # ── Sheets ────────────────────────────────────────────────────────────

    def _sheet_dq_score(self, writer, fmt):
        ws = writer.book.add_worksheet("DQ Score")
        ws.set_column("A:A", 30)
        ws.set_column("B:B", 20)
        ws.write(0, 0, "DATA QUALITY ASSESSMENT REPORT", fmt["title"])
        ws.write(1, 0, f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", fmt["subtitle"])
        ws.write(3, 0, "Overall DQ Score",  fmt["header"])
        ws.write(3, 1, f"{self.overall_score:.2f}%",
                 fmt["pass"] if self.overall_score >= 80 else fmt["fail"])
        ws.write(5, 0, "Total Records",   fmt["data"])
        ws.write(5, 1, len(self.results_df), fmt["data_center"])
        clean = len(self.results_df[self.results_df["Count of issues"] == 0])
        ws.write(6, 0, "Clean Records",  fmt["data"])
        ws.write(6, 1, clean,            fmt["data_center"])
        ws.write(7, 0, "Records with Issues", fmt["data"])
        ws.write(7, 1, len(self.results_df) - clean, fmt["data_center"])
        ws.write(9, 0, "Interpretation",  fmt["subtitle"])
        ws.write(10, 0, self._interpret(self.overall_score), fmt["data"])

    def _sheet_summary(self, writer, df_out, fmt, failed_tracker):
        ws = writer.book.add_worksheet("Column Summary")
        ws.write(0, 0, "COLUMN-LEVEL DQ SUMMARY", fmt["title"])
        headers = ["Column", "Total Records", "Failed Records", "DQ Score %", "Status"]
        for c, h in enumerate(headers):
            ws.write(2, c, h, fmt["header"])
            ws.set_column(c, c, 20)
        total = len(self.results_df)
        for r, col in enumerate(self.all_columns, 3):
            if col.startswith("_") or col in {"Issues", "Count of issues", "Failed_Rules", "Failed_Columns", "Issue categories"}:
                continue
            failed = len(failed_tracker.get(col, set()))
            score  = self.column_scores.get(col, 100.0)
            ws.write(r, 0, col, fmt["data"])
            ws.write(r, 1, total,  fmt["data_center"])
            ws.write(r, 2, failed, fmt["data_center"])
            ws.write(r, 3, f"{score:.2f}%", fmt["pass"] if score >= 80 else fmt["fail"])
            ws.write(r, 4, "PASS" if score >= 80 else "FAIL",
                     fmt["pass"] if score >= 80 else fmt["fail"])

    def _sheet_results(self, writer, df_out, fmt):
        ws = writer.book.add_worksheet("Detailed Results")
        ws.write(0, 0, "DETAILED VALIDATION RESULTS", fmt["title"])
        cols = [c for c in df_out.columns if not c.startswith("_")]
        for c, h in enumerate(cols):
            ws.write(2, c, h, fmt["header"])
            ws.set_column(c, c, 18)
        for r, (_, row) in enumerate(df_out.iterrows(), 3):
            issue_count = row.get("Count of issues", 0)
            row_fmt = fmt["fail"] if issue_count else fmt["pass"]
            for c, col in enumerate(cols):
                val = row[col]
                ws.write(r, c, str(val) if val is not None else "", row_fmt if col in ("Issues", "Count of issues") else fmt["data"])

    def _sheet_dimension(self, writer, fmt):
        ws = writer.book.add_worksheet("Dimension Scores")
        ws.write(0, 0, "DQ DIMENSION SCORES", fmt["title"])
        ws.write(2, 0, "Dimension",  fmt["header"])
        ws.write(2, 1, "Score %",    fmt["header"])
        ws.write(2, 2, "Status",     fmt["header"])
        ws.set_column(0, 2, 22)
        for r, (dim, score) in enumerate(self.dimension_scores.items(), 3):
            ws.write(r, 0, dim, fmt["data"])
            ws.write(r, 1, f"{score:.2f}%", fmt["data_center"])
            ws.write(r, 2, "PASS" if score >= 80 else "FAIL",
                     fmt["pass"] if score >= 80 else fmt["fail"])

    def _sheet_duplicate_summary(self, writer, fmt):
        ws = writer.book.add_worksheet("Duplicate Summary")
        ws.write(0, 0, "COMBINATION UNIQUENESS SUMMARY", fmt["title"])
        if not self.duplicate_combinations:
            ws.write(2, 0, "No combination uniqueness rules evaluated.", fmt["subtitle"])
            return
        ws.write(2, 0, "Column Combination", fmt["header"])
        ws.write(2, 1, "Duplicate Groups",   fmt["header"])
        ws.write(2, 2, "Total Dup Records",  fmt["header"])
        ws.set_column(0, 2, 30)
        for r, (combo, groups) in enumerate(self.duplicate_combinations.items(), 3):
            total_dup = sum(len(g) for g in groups)
            ws.write(r, 0, combo,       fmt["data"])
            ws.write(r, 1, len(groups), fmt["data_center"])
            ws.write(r, 2, total_dup,   fmt["data_center"])

    def _sheets_annexures(self, writer, df_out, fmt, failed_tracker):
        """Per-column annexure sheets for columns with failures."""
        display_cols = [c for c in df_out.columns if not c.startswith("_")]
        for col, indices in failed_tracker.items():
            if not indices:
                continue
            sheet_name = f"ANN_{col[:25]}"
            ws = writer.book.add_worksheet(sheet_name)
            ws.write(0, 0, f"ANNEXURE: {col}", fmt["title"])
            ws.write(1, 0, f"Failed Records: {len(indices)}", fmt["subtitle"])
            for c, h in enumerate(display_cols):
                ws.write(3, c, h, fmt["header"])
                ws.set_column(c, c, 18)
            for r, idx in enumerate(sorted(indices), 4):
                try:
                    record = df_out.iloc[idx]
                    for c, col_name in enumerate(display_cols):
                        ws.write(r, c, str(record[col_name]) if record[col_name] is not None else "", fmt["data"])
                except Exception:
                    continue

    def _sheet_uniqueness(self, writer, df_out, fmt, uniqueness_failures):
        ws = writer.book.add_worksheet("Uniqueness Issues")
        ws.write(0, 0, "UNIQUENESS VALIDATION — DUPLICATE RECORDS", fmt["title"])
        if not uniqueness_failures:
            ws.write(1, 0, "No duplicate records found", fmt["subtitle"])
            ws.write(2, 0, "Status: ✅ PASSED", fmt["pass"])
            return
        all_dup_indices: set = set()
        col_map: Dict[int, List[str]] = {}
        for col, indices in uniqueness_failures.items():
            for idx in indices:
                all_dup_indices.add(idx)
                col_map.setdefault(idx, []).append(col)
        ws.write(1, 0, f"Total Duplicate Records: {len(all_dup_indices)}", fmt["subtitle"])
        ws.write(2, 0, "Status: ❌ FAILED — DUPLICATES FOUND", fmt["fail"])
        display_cols = [c for c in df_out.columns if not c.startswith("_")]
        header_cols  = ["Row_Index", "Failed_Column", "Issue_Type"] + display_cols
        for c, h in enumerate(header_cols):
            ws.write(4, c, h, fmt["header"])
            ws.set_column(c, c, 18)
        for r, idx in enumerate(sorted(all_dup_indices), 5):
            try:
                ws.write(r, 0, idx, fmt["data"])
                ws.write(r, 1, ", ".join(col_map.get(idx, [])), fmt["data"])
                ws.write(r, 2, "Uniqueness Violation", fmt["fail"])
                record = df_out.iloc[idx]
                for c, col_name in enumerate(display_cols, 3):
                    ws.write(r, c, str(record[col_name]) if record[col_name] is not None else "", fmt["data"])
            except Exception:
                continue

    def _sheet_completeness(self, writer, df_out, fmt, dimension_tracker):
        indices: set = set()
        for col_set in dimension_tracker.get("Completeness", {}).values():
            indices.update(col_set)
        ws = writer.book.add_worksheet("Completeness Issues")
        ws.write(0, 0, "COMPLETENESS VALIDATION — MISSING VALUES", fmt["title"])
        if not indices:
            ws.write(1, 0, "No completeness issues found", fmt["subtitle"])
            ws.write(2, 0, "Status: ✅ PASSED", fmt["pass"])
            return
        ws.write(1, 0, f"Total Records with Missing Values: {len(indices)}", fmt["subtitle"])
        ws.write(2, 0, "Status: ❌ FAILED", fmt["fail"])
        display_cols = [c for c in df_out.columns if not c.startswith("_")]
        for c, h in enumerate(["Row_Index", "Incomplete_Columns"] + display_cols):
            ws.write(4, c, h, fmt["header"])
            ws.set_column(c, c, 20)
        for r, idx in enumerate(sorted(indices), 5):
            try:
                ws.write(r, 0, idx, fmt["data"])
                bad_cols = [rd["column"] for rd in (self.results_df.iloc[idx].get("_failed_rules_details") or [])
                            if isinstance(rd, dict) and rd.get("dimension") == "Completeness" and rd.get("column")]
                ws.write(r, 1, ", ".join(sorted(set(bad_cols))), fmt["data"])
                record = df_out.iloc[idx]
                for c, col_name in enumerate(display_cols, 2):
                    ws.write(r, c, str(record[col_name]) if record[col_name] is not None else "", fmt["data"])
            except Exception:
                continue

    def _sheet_standardization(self, writer, df_out, fmt, dimension_tracker):
        indices: set = set()
        for dim_key in ("Standardization", "Validation"):
            for col_set in dimension_tracker.get(dim_key, {}).values():
                indices.update(col_set)
        ws = writer.book.add_worksheet("Standardization Issues")
        ws.write(0, 0, "STANDARDIZATION VALIDATION — FORMAT ISSUES", fmt["title"])
        if not indices:
            ws.write(1, 0, "No standardization issues found", fmt["subtitle"])
            ws.write(2, 0, "Status: ✅ PASSED", fmt["pass"])
            return
        ws.write(1, 0, f"Total Records with Standardization Issues: {len(indices)}", fmt["subtitle"])
        ws.write(2, 0, "Status: ❌ FAILED", fmt["fail"])
        display_cols = [c for c in df_out.columns if not c.startswith("_")]
        for c, h in enumerate(["Row_Index", "Non_Standard_Columns"] + display_cols):
            ws.write(4, c, h, fmt["header"])
            ws.set_column(c, c, 20)
        for r, idx in enumerate(sorted(indices), 5):
            try:
                ws.write(r, 0, idx, fmt["data"])
                bad_cols = [rd["column"] for rd in (self.results_df.iloc[idx].get("_failed_rules_details") or [])
                            if isinstance(rd, dict) and rd.get("dimension") in ("Standardization", "Validation") and rd.get("column")]
                ws.write(r, 1, ", ".join(sorted(set(bad_cols))), fmt["data"])
                record = df_out.iloc[idx]
                for c, col_name in enumerate(display_cols, 2):
                    ws.write(r, c, str(record[col_name]) if record[col_name] is not None else "", fmt["data"])
            except Exception:
                continue

    @staticmethod
    def _interpret(score: float) -> str:
        if score >= 95:
            return "🎉 Excellent! Outstanding data quality."
        if score >= 80:
            return "👍 Good! Minor improvements needed."
        if score >= 60:
            return "⚠️ Fair! Significant improvements required."
        return "❌ Poor! Critical data quality issues detected."
