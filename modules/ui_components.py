import streamlit as st
import pandas as pd
import traceback
from typing import Dict
from .config import AppConfig


# ── Lottie CDN URLs (public domain / MIT animations) ──────────────────────
_LOTTIE_URLS = {
    "upload":     "https://assets10.lottiefiles.com/packages/lf20_jcikwtux.json",
    "processing": "https://assets9.lottiefiles.com/packages/lf20_ue6xppcm.json",
    "analytics":  "https://assets3.lottiefiles.com/packages/lf20_qp1q7mct.json",
    "success":    "https://assets3.lottiefiles.com/packages/lf20_pKiaUR.json",
}

_LOTTIE_JS_KEY = "_lottie_js_injected"


def _inject_lottie_lib() -> None:
    """Inject lottie-player web-component script once per session."""
    if st.session_state.get(_LOTTIE_JS_KEY):
        return
    st.markdown(
        '<script src="https://unpkg.com/@lottiefiles/lottie-player@latest'
        '/dist/lottie-player.js"></script>',
        unsafe_allow_html=True,
    )
    st.session_state[_LOTTIE_JS_KEY] = True


def _lottie_player(url: str, fallback_class: str, size: int = 120) -> str:
    return f"""
    <div class="orbit-loader"
         style="width:{size}px;height:{size}px;">
        <div class="center-dot"></div>
        <div class="orbiter"></div>
        <div class="orbiter"></div>
        <div class="orbiter"></div>
    </div>
    """
class UIComponents:
    """Streamlit UI components — with integrated animated guidance."""

    # ══════════════════════════════════════════════════════════════════════
    #  ORIGINAL HELPERS (unchanged)
    # ══════════════════════════════════════════════════════════════════════

    @staticmethod
    def render_header():
        st.markdown(
            f'<h1 class="purple-text">{AppConfig.APP_ICON} {AppConfig.APP_TITLE}</h1>',
            unsafe_allow_html=True,
        )
        st.caption(f"Version {AppConfig.VERSION}")
        st.markdown("---")

    @staticmethod
    def render_sidebar():
        with st.sidebar:
            st.markdown("### 📊 Supported Rules")
            st.markdown(
                "Completeness · Uniqueness · Validity · Standardization"
            )
            st.markdown("### 📁 File Formats")
            st.markdown("CSV · Excel · JSON · Parquet · ODS · XML · xlsx · xlsm · xlsb")

    @staticmethod
    def render_file_format_help():
        with st.expander("📋 Expected File Formats"):
            st.markdown("""
            ### Rules Dataset Format (CSV/Excel)

            **Required Columns:**
            - `column_name` or `column` — Target column name
            - `rule` or `rule_type` — Type of validation
            - `dimension` or `rule_category` — DQ dimension
            - `message` — Validation error message

            **Optional Columns:**
            - `expression` — Rule expression (regex, range, etc.)
            - `severity` — HIGH, MEDIUM, or LOW

            **Example:**
            ```csv
            column_name,rule,dimension,message,expression
            email,not_null,Completeness,Email is required,
            age,range,Validity,Age must be 0-120,"0,120"
            status,allowed_values,Validity,Invalid status,"Active,Inactive"
            ```

            ### JSON Rulebook Format
            ```json
            {
              "rules": [
                {
                  "column": "email",
                  "rule_type": "not_null",
                  "dimension": "Completeness",
                  "message": "Email is required",
                  "expression": null,
                  "severity": "HIGH"
                }
              ]
            }
            ```
            """)

    @staticmethod
    def render_results_dashboard(
        overall_score: float,
        results_df: pd.DataFrame,
        column_scores: Dict[str, float],
        dimension_scores: Dict[str, float],
    ):
        st.subheader("📊 Data Quality Dashboard")
        col1, col2, col3, col4 = st.columns(4)
        clean_count  = len(results_df[results_df["Count of issues"] == 0])
        issue_count  = len(results_df) - clean_count
        pass_columns = sum(1 for s in column_scores.values() if s == 100)

        col1.metric("Overall DQ Score",      f"{overall_score}%")
        col2.metric("Clean Records",         f"{clean_count:,}")
        col3.metric("Records with Issues",   f"{issue_count:,}")
        col4.metric("Columns at 100%",       f"{pass_columns}/{len(column_scores)}")

        if overall_score >= 95:
            st.success("🎉 **Excellent!** Outstanding data quality.")
        elif overall_score >= 80:
            st.info("👍 **Good!** Minor improvements needed.")
        elif overall_score >= 60:
            st.warning("⚠️ **Fair!** Significant improvements required.")
        else:
            st.error("❌ **Poor!** Critical data quality issues detected.")

    @staticmethod
    def render_download_section(output_path, rulebook_path, total_annexures: int):
        st.subheader("📥 Download Reports")
        col1, col2 = st.columns(2)
        with col1:
            with open(output_path, "rb") as f:
                st.download_button(
                    "📊 Download Complete Excel Report", f,
                    file_name=output_path.name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    help=f"Includes: DQ Score, Results, Summary, Dimension Analysis, and {total_annexures} Annexures",
                )
        with col2:
            with open(rulebook_path, "rb") as f:
                st.download_button(
                    "📋 Download Rulebook JSON", f,
                    file_name=rulebook_path.name,
                    mime="application/json",
                    use_container_width=True,
                    help="Generated rulebook for reference and reuse",
                )

    @staticmethod
    def render_detailed_views(
        rulebook: Dict,
        results_df: pd.DataFrame,
        column_scores: Dict[str, float],
        dimension_scores: Dict[str, float],
    ):
        st.subheader("🔍 Detailed Analysis")
        tab1, tab2, tab3, tab4 = st.tabs(
            ["Column Scores", "Dimension Analysis", "Rulebook", "Results Preview"]
        )
        with tab1:
            UIComponents._render_column_scores(column_scores)
        with tab2:
            UIComponents._render_dimension_scores(dimension_scores)
        with tab3:
            st.json(rulebook)
            st.info(f"Total rules: {len(rulebook.get('rules', []))}")
        with tab4:
            UIComponents._render_results_preview(results_df)

    @staticmethod
    def _render_column_scores(column_scores: Dict[str, float]):
        score_data = []
        for col, score in sorted(column_scores.items(), key=lambda x: x[1]):
            status = "✅ PASSED" if score == 100 else "❌ FAILED"
            score_data.append({"Column": col, "DQ Score (%)": score, "Status": status})
        score_df = pd.DataFrame(score_data)

        def color_status(val):
            if val == "✅ PASSED":
                return "background-color: #C6EFCE; color: #006100"
            return "background-color: #FFC7CE; color: #9C0006"

        st.dataframe(
            score_df.style.applymap(color_status, subset=["Status"]),
            use_container_width=True, hide_index=True,
        )

    @staticmethod
    def _render_dimension_scores(dimension_scores: Dict[str, float]):
        if not dimension_scores:
            st.info("No dimension analysis available")
            return
        dim_data = []
        for dimension, score in dimension_scores.items():
            status = "✅ PASSED" if score == 100 else "❌ FAILED"
            dim_data.append({"Dimension": dimension, "DQ Score (%)": score, "Status": status})
        st.dataframe(pd.DataFrame(dim_data), use_container_width=True, hide_index=True)

    @staticmethod
    def _render_results_preview(results_df: pd.DataFrame):
        display_cols = [c for c in results_df.columns if not c.startswith("_")]
        issues_df    = results_df[results_df["Count of issues"] > 0]
        if len(issues_df) > 0:
            st.write(f"**Total records with issues: {len(issues_df):,}**")
            st.dataframe(issues_df[display_cols].head(100), use_container_width=True)
        else:
            st.success("🎉 No issues found! All records passed validation.")
            st.dataframe(results_df[display_cols].head(20), use_container_width=True)

    @staticmethod
    def render_error_details(error: Exception):
        with st.expander("🔍 Show Detailed Error"):
            st.code(traceback.format_exc())

    @staticmethod
    def render_footer():
        st.markdown(
            '<div class="text-center margin-top-1">'
            '<p class="caption">Powered by '
            + AppConfig.APP_TITLE + " v" + AppConfig.VERSION
            + "</p></div>",
            unsafe_allow_html=True,
        )

    # ══════════════════════════════════════════════════════════════════════
    #  ANIMATED GUIDANCE — WORKFLOW TRACKER
    # ══════════════════════════════════════════════════════════════════════

    _WORKFLOW_STEPS = [
        {"icon": "📁", "label": "Upload Files",    "type": "upload"},
        {"icon": "🔧", "label": "Build Rulebook",  "type": "process"},
        {"icon": "✅", "label": "Run Rules",        "type": "process"},
        {"icon": "📊", "label": "Score & Analyse",  "type": "score"},
        {"icon": "📥", "label": "Export Reports",   "type": "export"},
    ]

    @staticmethod
    def render_workflow_tracker(active_step: int = 0) -> None:
        """
        Horizontal 5-step animated workflow tracker.
        active_step (0–4): current step.
        Steps before it → done (green). Current → animated. After → pending (grey).
        """
        steps = UIComponents._WORKFLOW_STEPS

        def _bubble_class(i: int) -> str:
            if i < active_step:
                return "done"
            if i == active_step:
                t = steps[i]["type"]
                return "active-upload" if t == "upload" else (
                    "processing" if t == "process" else "active"
                )
            return "pending"

        def _bubble_inner(i: int, cls: str) -> str:
            icon = steps[i]["icon"]
            if cls == "done":
                return '<span class="wf-check">✓</span>'
            if cls == "processing":
                return icon
            if cls == "active-upload":
                return f'<span class="wf-icon">{icon}</span>'
            return icon

        parts = ['<div class="wf-tracker">']
        for i, step in enumerate(steps):
            cls     = _bubble_class(i)
            inner   = _bubble_inner(i, cls)
            con_cls = "done" if i < active_step else ""
            parts.append(f"""
                <div class="wf-step">
                    <div class="wf-bubble {cls}">{inner}</div>
                    <span class="wf-label">{step['label']}</span>
                </div>
            """)
            if i < len(steps) - 1:
                parts.append(f'<div class="wf-connector {con_cls}"></div>')
        parts.append("</div>")
        st.markdown("".join(parts), unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════
    #  ANIMATED GUIDANCE — LOTTIE PLAYERS
    # ══════════════════════════════════════════════════════════════════════

    @staticmethod
    def render_lottie_upload(label: str = "Drop your file here") -> None:
        _inject_lottie_lib()
        player = _lottie_player(_LOTTIE_URLS["upload"], "lottie-upload-fallback")
        st.markdown(f"""
            <div class="lottie-slot">
                <div class="lottie-frame">{player}</div>
                <span class="lottie-caption">{label}</span>
            </div>
        """, unsafe_allow_html=True)

    @staticmethod
    def render_lottie_processing(label: str = "Processing your data…") -> None:
        _inject_lottie_lib()
        player = _lottie_player(_LOTTIE_URLS["processing"], "lottie-process-fallback")
        st.markdown(f"""
            <div class="lottie-slot">
                <div class="lottie-frame scan-overlay">{player}</div>
                <span class="lottie-caption">{label}</span>
            </div>
        """, unsafe_allow_html=True)

    @staticmethod
    def render_lottie_analytics(label: str = "Building your report…") -> None:
        _inject_lottie_lib()
        player = _lottie_player(_LOTTIE_URLS["analytics"], "lottie-analytics-fallback")
        st.markdown(f"""
            <div class="lottie-slot">
                <div class="lottie-frame">{player}</div>
                <span class="lottie-caption">{label}</span>
            </div>
        """, unsafe_allow_html=True)

    @staticmethod
    def render_lottie_success(label: str = "Assessment complete!") -> None:
        _inject_lottie_lib()
        player = _lottie_player(_LOTTIE_URLS["success"], "lottie-analytics-fallback")
        st.markdown(f"""
            <div class="lottie-slot">
                <div class="lottie-frame"
                     style="border-color:rgba(16,185,129,0.4);
                            box-shadow:0 0 30px rgba(16,185,129,0.2);">
                    {player}
                </div>
                <span class="lottie-caption" style="color:#34d399;">{label}</span>
            </div>
        """, unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════
    #  ANIMATED GUIDANCE — HINT PRIMITIVES
    # ══════════════════════════════════════════════════════════════════════

    @staticmethod
    def render_beacon(color: str = "#60a5fa") -> str:
        """Return beacon HTML (embed inside other HTML strings)."""
        return f"""
        <span class="beacon">
            <span class="beacon-dot" style="background:{color};"></span>
            <span class="beacon-ring" style="border-color:{color}80;"></span>
        </span>"""

    @staticmethod
    def render_hint_chip(label: str, tip: str = "", icon: str = "💡") -> None:
        tip_attr = f'data-tip="{tip}"' if tip else ""
        st.markdown(
            f'<span class="hint-chip" {tip_attr}>{icon} {label}</span>',
            unsafe_allow_html=True,
        )

    @staticmethod
    def render_action_hint_bar(
        title: str, message: str, color: str = "#60a5fa"
    ) -> None:
        """Animated beacon + text strip. message supports inline HTML."""
        beacon = UIComponents.render_beacon(color)
        st.markdown(f"""
            <div class="action-hint-bar">
                <div class="ahb-beacon">{beacon}</div>
                <div class="ahb-text">
                    <strong>{title}:</strong> {message}
                </div>
            </div>
        """, unsafe_allow_html=True)

    @staticmethod
    def render_arrow_down(color: str = "#60a5fa") -> None:
        st.markdown(
            f'<div class="guidance-arrow-down" style="color:{color};">↓</div>',
            unsafe_allow_html=True,
        )

    @staticmethod
    def render_guidance_card(
        icon: str,
        title: str,
        description: str,
        step_number: int | None = None,
        delay_ms: int = 0,
    ) -> None:
        """Shimmer guidance card with optional numbered badge."""
        badge = (
            f'<div class="guidance-card-step">{step_number}</div>'
            if step_number is not None else ""
        )
        st.markdown(f"""
            <div class="guidance-card" style="animation-delay:{delay_ms}ms;">
                {badge}
                <span class="guidance-card-icon">{icon}</span>
                <div class="guidance-card-title">{title}</div>
                <p class="guidance-card-desc">{description}</p>
            </div>
        """, unsafe_allow_html=True)

    @staticmethod
    def render_micro_progress(
        pct: int = 100,
        color_start: str = "#3b82f6",
        color_end: str = "#a78bfa",
    ) -> None:
        st.markdown(f"""
            <div class="micro-progress">
                <div class="micro-progress-fill"
                     style="--target-width:{pct}%;
                            background:linear-gradient(90deg,{color_start},{color_end});
                            background-size:200% 100%;">
                </div>
            </div>
        """, unsafe_allow_html=True)

    @staticmethod
    def render_pulsing_dot(color: str = "#f87171") -> None:
        st.markdown(f"""
            <div class="pulsing-dot" style="background:{color};"></div>
        """, unsafe_allow_html=True)
    @staticmethod
    def render_orbit_loader() -> None:
        st.markdown("""
            <div class="orbit-loader">
                <div class="center-dot"></div>
                <div class="orbiter"></div>
                <div class="orbiter"></div>
                <div class="orbiter"></div>
            </div>
        """, unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════
    #  ANIMATED GUIDANCE — COMPOSITE PANELS
    # ══════════════════════════════════════════════════════════════════════

    @staticmethod
    def render_upload_hint(file_type: str = "dataset") -> None:
        """
        Render a contextual animated hint strip directly above a file_uploader.
        Call this immediately BEFORE st.file_uploader().
        file_type: 'dataset' | 'rules'
        """
        if file_type == "dataset":
            icon, title, tip = "📋", "Master Dataset", "CSV, Excel, JSON, Parquet or ODS"
        else:
            icon, title, tip = "📜", "Rules Configuration", "CSV/Excel rules sheet or JSON rulebook"

        beacon = UIComponents.render_beacon()
        st.markdown(f"""
            <div class="action-hint-bar" style="margin-bottom:0.5rem;">
                <div class="ahb-beacon">{beacon}</div>
                <div class="ahb-text">
                    <strong>{icon} {title}</strong>&nbsp;
                    <span class="hint-chip"
                          style="font-size:0.72rem;padding:0.15rem 0.5rem;">
                        {tip}
                    </span>
                </div>
            </div>
        """, unsafe_allow_html=True)

    @staticmethod
    def render_welcome_screen() -> None:
        """Kept for backward compatibility — animations now live inline in page_dq()."""
        st.info("👆 Please upload both Master Dataset and Rules Configuration to begin")

    @staticmethod
    def render_results_header(score: float) -> None:
        """
        Animated header shown once DQ results are ready.
        Shows completed tracker + lottie success + action hint.
        """
        _inject_lottie_lib()
        UIComponents.render_workflow_tracker(active_step=4)

        color = "#10b981" if score >= 80 else ("#f59e0b" if score >= 60 else "#ef4444")
        emoji = "🎉"      if score >= 80 else ("👍"      if score >= 60 else "⚠️")

        _, col_c, _ = st.columns([1, 1.2, 1])
        with col_c:
            UIComponents.render_lottie_success(f"{emoji} Score: {score:.1f}%")

        UIComponents.render_action_hint_bar(
            title="Assessment complete",
            message="Download your <strong>Excel report</strong> below or "
                    "continue to the <strong>Maturity Assessment →</strong>",
            color=color,
        )