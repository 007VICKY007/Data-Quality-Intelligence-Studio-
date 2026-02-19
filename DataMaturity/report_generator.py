# ──────────────────────────────────────────────────────────────
#  IMPORTS
# ──────────────────────────────────────────────────────────────
from io import BytesIO
from datetime import datetime

import pandas as pd

from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
    Image,
)
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors


# ──────────────────────────────────────────────────────────────
#  MAIN PDF BUILDER
# ──────────────────────────────────────────────────────────────
def build_pdf_bytes(
    client_name: str,
    slide_png: bytes,
    dim_table: pd.DataFrame,
    overall: pd.Series,
    detail_tables: dict,
    dq_score: float = None,
) -> bytes:
    """
    Build full Data Maturity PDF report.

    Compatible with app.py submit flow.
    Returns PDF as bytes.
    """

    buffer = BytesIO()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        leftMargin=40,
        rightMargin=40,
        topMargin=40,
        bottomMargin=30,
    )

    styles = getSampleStyleSheet()
    story = []

    # ──────────────────────────────────────────────────────────
    # TITLE PAGE
    # ──────────────────────────────────────────────────────────
    story.append(
        Paragraph(
            f"<b>Data Maturity Assessment Report</b>",
            styles["Title"],
        )
    )

    story.append(
        Paragraph(
            f"Client: <b>{client_name}</b>",
            styles["Heading2"],
        )
    )

    story.append(
        Paragraph(
            f"Generated on: {datetime.now().strftime('%d %b %Y, %H:%M')}",
            styles["Normal"],
        )
    )

    story.append(Spacer(1, 20))

    # ──────────────────────────────────────────────────────────
    # SLIDE IMAGE
    # ──────────────────────────────────────────────────────────
    story.append(
        Image(
            BytesIO(slide_png),
            width=720,
            height=405,
        )
    )

    story.append(PageBreak())

    # ──────────────────────────────────────────────────────────
    # DQ LINKAGE (OPTIONAL)
    # ──────────────────────────────────────────────────────────
    if dq_score is not None:

        story.append(
            Paragraph("DQ Engine Linkage", styles["Heading1"])
        )

        dq_level = "Mapped via DQ → Maturity Model"

        dq_data = [
            ["Metric", "Value"],
            ["DQ Overall Score", f"{dq_score:.2f}%"],
            ["Mapping Note", dq_level],
        ]

        table = Table(dq_data, colWidths=[300, 300])

        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#5b2d90")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ]
            )
        )

        story.append(table)
        story.append(PageBreak())

    # ──────────────────────────────────────────────────────────
    # DIMENSION SUMMARY
    # ──────────────────────────────────────────────────────────
    story.append(
        Paragraph("Dimension-wise Maturity Scores", styles["Heading1"])
    )

    dim_df = dim_table.reset_index()
    dim_data = [list(dim_df.columns)] + dim_df.values.tolist()

    table = Table(dim_data, repeatRows=1)

    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#5b2d90")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ]
        )
    )

    story.append(table)
    story.append(Spacer(1, 20))

    # ──────────────────────────────────────────────────────────
    # OVERALL SUMMARY
    # ──────────────────────────────────────────────────────────
    story.append(
        Paragraph("Overall Maturity Scores", styles["Heading1"])
    )

    ov_df = pd.DataFrame({
        "Master Data Object": list(overall.index),
        "Score": list(overall.values),
    })

    ov_data = [list(ov_df.columns)] + ov_df.values.tolist()

    table = Table(ov_data, repeatRows=1)

    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#5b2d90")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ]
        )
    )

    story.append(table)
    story.append(PageBreak())

    # ──────────────────────────────────────────────────────────
    # DETAIL TABLES
    # ──────────────────────────────────────────────────────────
    for dim, df in detail_tables.items():

        story.append(
            Paragraph(f"Detailed Responses – {dim}", styles["Heading1"])
        )

        data = [list(df.columns)] + df.head(30).values.tolist()

        table = Table(data, repeatRows=1)

        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#5b2d90")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ]
            )
        )

        story.append(table)
        story.append(PageBreak())

    # ──────────────────────────────────────────────────────────
    # BUILD PDF
    # ──────────────────────────────────────────────────────────
    doc.build(story)

    pdf_bytes = buffer.getvalue()
    buffer.close()

    return pdf_bytes