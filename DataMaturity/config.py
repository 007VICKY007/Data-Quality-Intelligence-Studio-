# ──────────────────────────────────────────────────────────────
#  DATA MATURITY CONFIGURATION — Uniqus Light Corporate Theme
# ──────────────────────────────────────────────────────────────

# ══════════════════════════════════════════════════════════════
# 🎨 THEME COLORS — Light Corporate Palette
# ══════════════════════════════════════════════════════════════

UNIQU_PURPLE   = "#5b2d90"   # Deep brand purple
UNIQU_MAGENTA  = "#b10f74"   # Brand magenta/pink
UNIQU_LAVENDER = "#ede8f7"   # Light purple tint

UNIQU_LIGHT_BG = "#ffffff"   # White background
UNIQU_TEXT     = "#1a1a2e"   # Charcoal text
UNIQU_GREY     = "#d9cef0"   # Light purple-grey border

# Extended palette
UNIQU_PURPLE_DARK  = "#3d1d63"
UNIQU_PURPLE_MID   = "#7c4dbb"
UNIQU_PURPLE_LIGHT = "#ede8f7"
UNIQU_PURPLE_PALE  = "#f5f0fc"
UNIQU_SURFACE      = "#f9f8fc"

# ══════════════════════════════════════════════════════════════
# 📊 MATURITY RATING SCALE
# ══════════════════════════════════════════════════════════════

RATING_LABELS = [
    "Adhoc",
    "Repeatable",
    "Defined",
    "Managed",
    "Optimised",
]

RATING_TO_SCORE = {
    "Adhoc":       1,
    "Repeatable":  2,
    "Defined":     3,
    "Managed":     4,
    "Optimised":   5,
}

# ══════════════════════════════════════════════════════════════
# 🔗 DQ → MATURITY MAPPING
# ══════════════════════════════════════════════════════════════
DQ_MATURITY_MAP = [
    (95, "Optimised"),
    (80, "Managed"),
    (60, "Defined"),
    (40, "Repeatable"),
    (0,  "Adhoc"),
]

# ══════════════════════════════════════════════════════════════
# 🏢 DEFAULT MASTER DATA OBJECTS
# ══════════════════════════════════════════════════════════════

DEFAULT_MASTER_OBJECTS = [
    "Customer",
    "Vendor",
    "Material",
    "Finance",
    "Employee",
]

# ══════════════════════════════════════════════════════════════
# 🧭 MATURITY DIMENSIONS (DAMA-Aligned)
# ══════════════════════════════════════════════════════════════

MATURITY_DIMS = [
    "Data Governance",
    "Data Quality",
    "Data Integration & Interoperability",
]

# ══════════════════════════════════════════════════════════════
# ❓ COMPREHENSIVE QUESTION BANK (DAMA-Based)
# ══════════════════════════════════════════════════════════════

QUESTION_BANK = {
    "Data Governance": [
        {"id": "DG-1",  "section": "Data Management Strategy (DMS)", "question": "Documented Data Management Strategy exists (vision, scope, objectives).", "weight": 2},
        {"id": "DG-2",  "section": "Data Management Strategy (DMS)", "question": "Stakeholders are involved in strategy creation and review.", "weight": 1},
        {"id": "DG-3",  "section": "Data Management Strategy (DMS)", "question": "Strategy is approved, published, and communicated to relevant stakeholders.", "weight": 1},
        {"id": "DG-4",  "section": "Roles & Responsibilities", "question": "Data roles (Owner, Steward, Custodian) are defined for the object.", "weight": 2},
        {"id": "DG-5",  "section": "Roles & Responsibilities", "question": "Roles and responsibilities are documented and communicated.", "weight": 1},
        {"id": "DG-6",  "section": "Policies & Standards", "question": "Governance policies/standards exist (naming, definitions, approvals).", "weight": 2},
        {"id": "DG-7",  "section": "Policies & Standards", "question": "Policies are periodically reviewed and updated.", "weight": 1},
        {"id": "DG-8",  "section": "DMO", "question": "Data Management Office (DMO) / governance forum exists.", "weight": 2},
        {"id": "DG-9",  "section": "DMO", "question": "Operating model & governance cadence are defined (RACI, forums, KPIs).", "weight": 1},
        {"id": "DG-10", "section": "Change Management", "question": "Change control process exists for master data requests/updates.", "weight": 2},
        {"id": "DG-11", "section": "Change Management", "question": "Training / enablement exists for users and data stewards.", "weight": 1},
        {"id": "DG-12", "section": "Issue Management", "question": "Issue logging, triage, and resolution workflow exists.", "weight": 1},
        {"id": "DG-13", "section": "Issue Management", "question": "Root-cause analysis and lessons learned are captured.", "weight": 1},
        {"id": "DG-14", "section": "Metadata", "question": "Metadata (definitions, owners, rules) is managed in a repository/catalog.", "weight": 1},
    ],
    "Data Quality": [
        {"id": "DQ-1",  "section": "Assessment & Rules", "question": "Data quality assessment policy exists (what, how often, ownership).", "weight": 2},
        {"id": "DQ-2",  "section": "Assessment & Rules", "question": "DQ rules are defined (completeness, validity, uniqueness, consistency).", "weight": 2},
        {"id": "DQ-3",  "section": "Assessment & Rules", "question": "DQ rules cover critical fields and are documented with thresholds.", "weight": 2},
        {"id": "DQ-4",  "section": "Monitoring", "question": "DQ monitoring is periodic and tracked (dashboards/scorecards).", "weight": 2},
        {"id": "DQ-5",  "section": "Monitoring", "question": "Automated validation exists (API checks, format checks, reference checks).", "weight": 1},
        {"id": "DQ-6",  "section": "Duplicates", "question": "Duplicate detection & golden record process exists.", "weight": 2},
        {"id": "DQ-7",  "section": "Profiling", "question": "Data profiling is performed using tools/standard techniques.", "weight": 1},
        {"id": "DQ-8",  "section": "Profiling", "question": "Anomalies/inconsistencies are identified and resolved consistently.", "weight": 1},
        {"id": "DQ-9",  "section": "Standardization", "question": "Standardization rules exist (formats, naming conventions, codes).", "weight": 2},
        {"id": "DQ-10", "section": "Standardization", "question": "Uniform definitions and formatting are applied across datasets/systems.", "weight": 1},
        {"id": "DQ-11", "section": "Cleansing", "question": "Cleansing workflow/tools exist (issue queues, approvals, audit trail).", "weight": 2},
        {"id": "DQ-12", "section": "Cleansing", "question": "Recurring cleansing is planned (not only ad-hoc one-time fixes).", "weight": 1},
    ],
    "Data Integration & Interoperability": [
        {"id": "DI-1", "section": "Integration Strategy & Architecture", "question": "Enterprise-wide integration strategy exists (APIs/ETL/events), aligned to target state.", "weight": 2},
        {"id": "DI-2", "section": "Integration Strategy & Architecture", "question": "System of Record (SoR) / System of Entry is clearly defined per object.", "weight": 2},
        {"id": "DI-3", "section": "Integration Strategy & Architecture", "question": "Integration flows and interfaces are documented (source-to-target mapping).", "weight": 2},
        {"id": "DI-4", "section": "Integration Technology & Tools", "question": "Integration platform/tooling supports scalability & performance requirements.", "weight": 1},
        {"id": "DI-5", "section": "Integration Technology & Tools", "question": "Logging, monitoring, reconciliation, and audit trails exist for data movement.", "weight": 2},
    ],
}
