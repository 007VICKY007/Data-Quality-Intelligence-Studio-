"""
Enterprise Maturity Slide Visualization
"""

import numpy as np
from io import BytesIO
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.patches import Wedge, Circle, Rectangle

UNIQU_PURPLE = "#6A0DAD"
UNIQU_MAGENTA = "#FF00FF"
UNIQU_LIGHT_BG = "#F8F5FF"
UNIQU_TEXT = "#2E2E2E"
UNIQU_GREY = "#B0B0B0"


# ─────────────────────────────
# Helpers
# ─────────────────────────────
def safe_float(v):
    try:
        return float(v)
    except:
        return np.nan


def safe_rating(v):
    try:
        return int(round(float(v)))
    except:
        return 0


# ─────────────────────────────
# Maturity Wheel
# ─────────────────────────────
def _draw_maturity_wheel(ax, center=(0.18, 0.44), r_outer=0.20, r_inner=0.09):

    colors = ["#d9cff2", "#bda7ea", "#8f63d7", "#4a1f7a", "#1d0b3f"]

    n = 5
    ts, te = -90, 90
    step = (te - ts) / n

    for i in range(n):
        si = (n - 1) - i
        t1 = ts + si * step
        t2 = ts + (si + 1) * step

        ax.add_patch(Wedge(
            center, r_outer, t1, t2,
            width=(r_outer - r_inner),
            transform=ax.transAxes,
            facecolor=colors[i],
            edgecolor="white",
            linewidth=1.2
        ))

    ax.add_patch(Circle(
        center, r_inner - 0.005,
        transform=ax.transAxes,
        facecolor="#9a86c6",
        edgecolor="white"
    ))

    ax.text(
        center[0], center[1],
        "Data\nManagement\nMaturity Scale",
        transform=ax.transAxes,
        ha="center", va="center",
        fontsize=9,
        color="white",
        fontweight="bold"
    )


# ─────────────────────────────
# Domain Table
# ─────────────────────────────
def _draw_domain_table(ax, scores):

    x, y, w, h = 0.52, 0.52, 0.38, 0.23

    ax.add_patch(Rectangle((x, y), w, h,
                           transform=ax.transAxes,
                           facecolor="white",
                           edgecolor=UNIQU_GREY))

    hh = 0.07
    ax.add_patch(Rectangle((x, y + h - hh), w, hh,
                           transform=ax.transAxes,
                           facecolor="#c07bb3"))

    ax.text(x + 0.02, y + h - hh / 2,
            "Data Management Framework Domains",
            fontsize=9,
            fontweight="bold",
            transform=ax.transAxes,
            va="center")

    rows = list(scores.keys())
    row_h = (h - hh) / len(rows)

    for i, r in enumerate(rows):

        val = scores[r]
        yy = y + h - hh - (i + 1) * row_h + 0.02

        ax.text(x + 0.02, yy, r,
                fontsize=9,
                transform=ax.transAxes,
                color=UNIQU_TEXT)

        ax.text(x + 0.26, yy, f"{val:.1f}",
                fontsize=9,
                transform=ax.transAxes)


# ─────────────────────────────
# Donut
# ─────────────────────────────
def _draw_donut(ax, center, value, label, color):

    r = 0.04

    ax.add_patch(Wedge(
        center, r, 0, 360,
        width=0.01,
        transform=ax.transAxes,
        facecolor="#efe9f7"
    ))

    frac = value / 5

    ax.add_patch(Wedge(
        center, r,
        90 - 360 * frac, 90,
        width=0.01,
        transform=ax.transAxes,
        facecolor=color
    ))

    ax.text(center[0], center[1],
            f"{value:.2f}",
            transform=ax.transAxes,
            ha="center", va="center",
            fontsize=11,
            fontweight="bold")

    ax.text(center[0], center[1] + 0.08,
            label,
            transform=ax.transAxes,
            ha="center",
            fontsize=8)


# ─────────────────────────────
# MAIN SLIDE
# ─────────────────────────────
def render_summary_slide_png(
    client_name,
    domain_scores,
    exec_score,
    benchmark,
    target
):

    fig = plt.figure(figsize=(13.6, 7.65), dpi=160)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")

    fig.patch.set_facecolor(UNIQU_LIGHT_BG)

    # Title
    ax.text(0.05, 0.92,
            "Data Maturity Level",
            fontsize=22,
            fontweight="bold",
            color=UNIQU_PURPLE,
            transform=ax.transAxes)

    # Brand
    ax.add_patch(Rectangle(
        (0.52, 0.92), 0.40, 0.008,
        transform=ax.transAxes,
        facecolor=UNIQU_PURPLE))

    ax.text(0.90, 0.94,
            "uniqus",
            fontsize=16,
            fontweight="bold",
            color=UNIQU_PURPLE,
            transform=ax.transAxes)

    # Wheel
    _draw_maturity_wheel(ax)

    # Table
    _draw_domain_table(ax, domain_scores)

    # Donuts
    _draw_donut(ax, (0.60, 0.22), exec_score, client_name, UNIQU_PURPLE)
    _draw_donut(ax, (0.72, 0.22), benchmark, "Benchmark", UNIQU_MAGENTA)
    _draw_donut(ax, (0.84, 0.22), target, "Target", "#a083c9")

    # Footer
    ax.text(0.05, 0.04,
            f"Data Maturity Assessment Report for {client_name}",
            fontsize=10,
            transform=ax.transAxes)

    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches=None, pad_inches=0.0)
    plt.close(fig)

    return buf.getvalue()