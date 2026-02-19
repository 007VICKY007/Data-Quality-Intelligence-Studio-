"""
DataMaturity/visualizations.py
===============================
Matplotlib-based slide-style summary page containing:
  • Maturity Wheel  – right semicircle (01 top → 05 bottom) with callouts
  • Domain Table    – DAMA domains with mini maturity bar charts
  • Donut Scores    – client / benchmark / target indicators

Public API
----------
render_slide_png(client_name, domain_scores, exec_score, benchmark, target) → bytes
"""

import numpy as np
from io import BytesIO
from datetime import datetime

import matplotlib.pyplot as plt
from matplotlib.patches import Wedge, Circle, Rectangle

from DataMaturity.config import (
    UNIQU_PURPLE, UNIQU_MAGENTA, UNIQU_LIGHT_BG, UNIQU_TEXT, UNIQU_GREY,
)
from DataMaturity.helpers import safe_float, safe_rating


# ──────────────────────────────────────────────────────────────
# Internal: Maturity Wheel
# ──────────────────────────────────────────────────────────────
def _draw_maturity_wheel(ax) -> None:
    """
    Draw a right-semicircle ring (01 at TOP, 05 at BOTTOM) with
    L-shaped callout connectors.  Coordinates are in ax.transAxes space.
    """
    cx, cy   = 0.10, 0.42
    ro, ri   = 0.180, 0.080
    magenta  = "#b0127b"
    seg_col  = ["#d9cff2", "#bda7ea", "#8f63d7", "#4a1f7a", "#1d0b3f"]

    n  = 5
    ts = -90.0           # bottom of right semicircle
    te = +90.0           # top
    step = (te - ts) / n  # 36 °

    # ── Ring segments ─────────────────────────────────────────
    for i in range(n):
        si = (n - 1) - i          # segment index (reversed so 01=top)
        t1 = ts + si * step
        t2 = ts + (si + 1) * step
        ax.add_patch(Wedge(
            (cx, cy), ro, t1, t2,
            width=(ro - ri), transform=ax.transAxes,
            facecolor=seg_col[i], edgecolor="white", linewidth=1.2, zorder=3,
        ))

    # ── Inner circle + label ──────────────────────────────────
    ax.add_patch(Circle(
        (cx, cy), ri - 0.005, transform=ax.transAxes,
        facecolor="#9a86c6", edgecolor="white", linewidth=1.2, zorder=4,
    ))
    ax.text(cx, cy, "Data\nManagement\nMaturity Scale",
            transform=ax.transAxes, ha="center", va="center",
            fontsize=8.2, color="white", fontweight="bold",
            linespacing=1.15, zorder=5)

    # ── Numeric labels 01–05 on the ring ─────────────────────
    for i in range(n):
        si  = (n - 1) - i
        mid = ts + (si + 0.5) * step
        rad = np.deg2rad(mid)
        rm  = (ro + ri) / 2
        ax.text(
            cx + rm * np.cos(rad), cy + rm * np.sin(rad),
            f"{i + 1:02d}", transform=ax.transAxes,
            ha="center", va="center", fontsize=14,
            color="white" if i >= 3 else "#6d6d6d", zorder=6,
        )

    # ── L-shaped callout connectors ───────────────────────────
    callouts = [
        ("Initial/Ad Hoc",
         "Processes are unstructured, reactive,\nand vary widely across the organization",
         0.70, 0),
        ("Repeatable",
         "Some processes are defined, but they are\ninconsistent and may lack formal documentation",
         0.55, 1),
        ("Defined",
         "Processes are standardized, documented,\nand consistently followed",
         0.41, 2),
        ("Managed",
         "Processes are monitored and measured. Data governance\nroles are formalized, and there's accountability",
         0.23, 3),
        ("Optimized",
         "Continuous improvement practices are in place, with proactive data\nquality, security, and governance strategies",
         0.10, 4),
    ]

    x_bus = cx + ro + 0.045   # vertical bus x
    x_dot = 0.378              # dot before text
    x_txt = 0.39               # text start

    for title, desc, y_t, idx in callouts:
        si  = (n - 1) - idx
        mid = ts + (si + 0.5) * step
        rad = np.deg2rad(mid)
        x0  = cx + ro * np.cos(rad)
        y0  = cy + ro * np.sin(rad)

        # Horizontal → vertical → horizontal line
        ax.plot([x0, x_bus], [y0, y0],   transform=ax.transAxes,
                color=magenta, lw=1.2, zorder=2)
        ax.plot([x_bus, x_bus], [y0, y_t], transform=ax.transAxes,
                color=magenta, lw=1.2, zorder=2)
        ax.plot([x_bus, x_dot], [y_t, y_t], transform=ax.transAxes,
                color=magenta, lw=1.2, zorder=2)

        ax.scatter([x_dot], [y_t], transform=ax.transAxes,
                   s=18, color="white", edgecolor=magenta, linewidth=1.2, zorder=3)

        ax.text(x_txt, y_t + 0.012, title,
                transform=ax.transAxes, ha="left", va="bottom",
                fontsize=8.2, fontweight="bold", color=UNIQU_TEXT, zorder=3)
        ax.text(x_txt, y_t - 0.006, desc,
                transform=ax.transAxes, ha="left", va="top",
                fontsize=7.6, color="#444444", linespacing=1.15, zorder=3)


# ──────────────────────────────────────────────────────────────
# Internal: Domain Score Table (right panel)
# ──────────────────────────────────────────────────────────────
def _draw_domain_table(ax, domain_scores: dict) -> None:
    """Right-panel table: domain name | score value | mini bar chart."""
    x, y, w, h = 0.60, 0.52, 0.35, 0.23

    # Outer box
    ax.add_patch(Rectangle((x, y), w, h, transform=ax.transAxes,
                            facecolor="white", edgecolor=UNIQU_GREY, linewidth=1))

    # Header
    hh = 0.07
    ax.add_patch(Rectangle((x, y + h - hh), w, hh, transform=ax.transAxes,
                            facecolor="#c07bb3", edgecolor="none", alpha=0.95))
    ax.text(x + 0.02, y + h - hh / 2,
            "Data Management Framework\nDomains",
            transform=ax.transAxes, fontsize=9,
            color="black", fontweight="bold", va="center")
    ax.text(x + 0.22, y + h - hh / 2, "DAMA Maturity Level",
            transform=ax.transAxes, fontsize=9,
            color="black", fontweight="bold", va="center")

    rows   = list(domain_scores.keys())
    avail  = h - hh - 0.016
    row_h  = avail / max(len(rows), 1)
    y_top  = y + h - hh - 0.008

    for i, rname in enumerate(rows):
        v   = domain_scores[rname]
        rt  = y_top - i * row_h
        rm  = rt - row_h / 2

        ax.plot([x, x + w], [rt, rt], transform=ax.transAxes,
                color=UNIQU_GREY, lw=1)

        ax.text(x + 0.02, rm, rname,
                transform=ax.transAxes, fontsize=8.6,
                color=UNIQU_TEXT, va="center", linespacing=1.1)

        vv   = safe_float(v)
        vtxt = "N/A" if not np.isfinite(vv) else f"{vv:.1f}"
        ax.text(x + 0.185, rm, vtxt,
                transform=ax.transAxes, fontsize=8.8,
                color=UNIQU_TEXT, va="center")

        # Mini maturity bar (5 blocks)
        bx, bw  = x + 0.245, 0.105
        bh      = min(0.028, row_h * 0.45)
        by      = rm - bh / 2
        gap     = 0.004
        nb      = 5
        bkw     = (bw - gap * (nb - 1)) / nb

        for b in range(nb):
            ax.add_patch(Rectangle(
                (bx + b * (bkw + gap), by), bkw, bh,
                transform=ax.transAxes, facecolor="#e9e2f4", edgecolor="none"))

        for b in range(safe_rating(v)):
            ax.add_patch(Rectangle(
                (bx + b * (bkw + gap), by), bkw, bh,
                transform=ax.transAxes,
                facecolor=UNIQU_PURPLE if b >= 3 else "#9a79d4",
                edgecolor="none"))

        # Position marker
        if np.isfinite(vv):
            mx = bx + float(np.clip((vv - 1) / 4, 0.0, 1.0)) * bw
            ax.plot([mx, mx], [by - 0.01, by + bh + 0.01],
                    transform=ax.transAxes, color="#2a2a2a", lw=1)

    ax.plot([x, x + w], [y, y], transform=ax.transAxes,
            color=UNIQU_GREY, lw=1)


# ──────────────────────────────────────────────────────────────
# Internal: Donut Score Indicator
# ──────────────────────────────────────────────────────────────
def _draw_donut(ax, center: tuple, value: float, label: str, color: str) -> None:
    r, t = 0.04, 0.01
    ax.add_patch(Wedge(center, r, 0, 360, width=t,
                       transform=ax.transAxes,
                       facecolor="#efe9f7", edgecolor="none"))
    frac = float(np.clip(value / 5.0, 0, 1))
    ax.add_patch(Wedge(center, r, 90 - 360 * frac, 90, width=t,
                       transform=ax.transAxes,
                       facecolor=color, edgecolor="none"))
    ax.text(center[0], center[1], f"{value:.2f}",
            transform=ax.transAxes, ha="center", va="center",
            fontsize=11, fontweight="bold", color=UNIQU_TEXT)
    ax.text(center[0], center[1] + 0.085, label,
            transform=ax.transAxes, ha="center", va="center",
            fontsize=8.5, color=UNIQU_TEXT, fontweight="bold")


# ──────────────────────────────────────────────────────────────
# Public: Render full summary slide → PNG bytes
# ──────────────────────────────────────────────────────────────
def render_slide_png(
    client_name:   str,
    domain_scores: dict,
    exec_score:    float,
    benchmark:     float,
    target:        float,
) -> bytes:
    """
    Build and return PNG bytes of the full maturity summary slide.

    Parameters
    ----------
    client_name   : Organisation name (header + footer)
    domain_scores : {label: float_1to5}  – displayed in right-panel table
    exec_score    : Client overall score (1-5) for left donut
    benchmark     : Industry benchmark score (1-5)
    target        : Target maturity score (1-5)
    """
    fig = plt.figure(figsize=(13.6, 7.65), dpi=160)
    fig.subplots_adjust(0, 0, 1, 1)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")
    fig.patch.set_facecolor(UNIQU_LIGHT_BG)

    # ── Title & subtitle ──────────────────────────────────────
    ax.text(0.05, 0.92, "Data Maturity Level",
            transform=ax.transAxes, fontsize=20,
            fontweight="bold", color=UNIQU_PURPLE)
    ax.text(
        0.05, 0.875,
        "Maturity level assesses an organization's data management capabilities "
        "across the key domains. This report summarizes the current maturity "
        "assessment aligned to DAMA principles.",
        transform=ax.transAxes, fontsize=9, color="#444444",
    )

    # ── Brand accent lines + logo ─────────────────────────────
    ax.add_patch(Rectangle((0.52, 0.920), 0.43, 0.008,
                            transform=ax.transAxes,
                            facecolor=UNIQU_PURPLE, edgecolor="none"))
    ax.add_patch(Rectangle((0.52, 0.912), 0.43, 0.004,
                            transform=ax.transAxes,
                            facecolor=UNIQU_MAGENTA, edgecolor="none"))
    ax.text(0.90, 0.94, "uniqus",
            transform=ax.transAxes, fontsize=16,
            color=UNIQU_PURPLE, fontweight="bold")

    # ── Left panel: maturity wheel ────────────────────────────
    _draw_maturity_wheel(ax)

    # ── Right panel: domain scores table ─────────────────────
    _draw_domain_table(ax, domain_scores)

    # ── Bottom donuts ─────────────────────────────────────────
    client_label = client_name.strip() if client_name.strip() else "Client Score"
    _draw_donut(ax, (0.66, 0.22), exec_score,  client_label,          UNIQU_PURPLE)
    _draw_donut(ax, (0.76, 0.22), benchmark,   "Industry\nBenchmark", UNIQU_MAGENTA)
    _draw_donut(ax, (0.88, 0.22), target,       "Target",              "#a083c9")

    # ── Footer ───────────────────────────────────────────────
    cn = client_name.strip() or "Client"
    ax.text(0.05, 0.04, f"Data Maturity Assessment Report for {cn}",
            transform=ax.transAxes, fontsize=10,
            color="#555555", fontweight="bold")
    ax.text(0.05, 0.02,
            f"Generated on: {datetime.now().strftime('%d %b %Y, %H:%M')}",
            transform=ax.transAxes, fontsize=8.5, color="#666666")

    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches=None, pad_inches=0.0)
    plt.close(fig)
    return buf.getvalue()