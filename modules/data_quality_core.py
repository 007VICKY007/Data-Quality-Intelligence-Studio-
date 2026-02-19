"""
modules/data_quality_core.py
══════════════════════════════════════════════════════════════════════════
Merged module: dq_engine + rule_executor + rulebook_builder

Contains:
  • RulebookBuilderService  — builds/loads rulebooks from CSV/Excel/JSON
  • RuleExecutorEngine      — executes rules with full column/dimension tracking
  • DataQualityEngine       — high-level DQ orchestrator (thin wrapper)
══════════════════════════════════════════════════════════════════════════
"""

import re
import json
import datetime
import pandas as pd
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Optional

from modules.config import RULE_ALIAS_MAP


# ══════════════════════════════════════════════════════════════════════════
#  RULEBOOK BUILDER
# ══════════════════════════════════════════════════════════════════════════

class RulebookBuilderService:
    """Converts rules datasets to JSON rulebook or loads existing JSON.
    Supports combination uniqueness rules (e.g. 'column1 + column2').
    """

    def __init__(self):
        self.rule_alias_map = RULE_ALIAS_MAP

    # ── Public ────────────────────────────────────────────────────────────

    def load_json_rulebook(self, file_path: Path) -> Dict:
        """Load an existing JSON rulebook."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                rulebook = json.load(f)
            if "rules" not in rulebook or not isinstance(rulebook["rules"], list):
                raise ValueError("Rulebook must contain 'rules' array")
            return rulebook
        except Exception as e:
            raise Exception(f"Error loading JSON rulebook: {str(e)}")

    def build_from_rules_dataset(
        self,
        rules_df: pd.DataFrame,
        base_columns: List[str],
    ) -> Dict:
        """Build a rulebook dict from a rules spreadsheet/dataframe.

        Expected columns (flexible naming):
          column_name | column   — target column (supports 'col1 + col2' combos)
          rule_type   | rule     — validation type
          dimension   | rule_category — DQ dimension
          message                — error message
          expression             — optional expression/parameter
          severity               — HIGH / MEDIUM / LOW
        """
        rules = []
        col_name_field   = self._detect_column_field(rules_df)
        rule_type_field  = self._detect_rule_field(rules_df)
        dimension_field  = self._detect_dimension_field(rules_df)

        for _, row in rules_df.iterrows():
            column = row.get(col_name_field)
            if not column or pd.isna(column):
                continue

            if "+" in str(column):
                rule = self._build_combination_rule(
                    row, column, rule_type_field, dimension_field, base_columns
                )
            else:
                col = str(column).strip()
                rule = (
                    self._build_single_rule(row, col, rule_type_field, dimension_field)
                    if col in base_columns else None
                )

            if rule:
                rules.append(rule)

        return {
            "rules": rules,
            "metadata": {
                "created": datetime.datetime.now().isoformat(),
                "total_rules": len(rules),
                "source": "rules_dataset",
            },
        }

    # ── Field detectors ────────────────────────────────────────────────

    def _detect_column_field(self, df: pd.DataFrame) -> str:
        if "column_name" in df.columns:
            return "column_name"
        if "column" in df.columns:
            return "column"
        raise ValueError("Rules dataset must have 'column_name' or 'column' field")

    def _detect_rule_field(self, df: pd.DataFrame) -> str:
        if "rule_type" in df.columns:
            return "rule_type"
        if "rule" in df.columns:
            return "rule"
        raise ValueError("Rules dataset must have 'rule_type' or 'rule' field")

    def _detect_dimension_field(self, df: pd.DataFrame) -> Optional[str]:
        if "dimension" in df.columns:
            return "dimension"
        if "rule_category" in df.columns:
            return "rule_category"
        return None

    # ── Rule builders ─────────────────────────────────────────────────

    def _build_combination_rule(
        self,
        row: pd.Series,
        column_combination: str,
        rule_field: str,
        dimension_field: Optional[str],
        base_columns: List[str],
    ) -> Optional[Dict]:
        columns = [c.strip() for c in str(column_combination).split("+")]
        valid_columns = [c for c in columns if c in base_columns]
        if len(valid_columns) < 2:
            return None

        rule_value = row.get(rule_field)
        rule_type  = "uniqueness" if not rule_value or pd.isna(rule_value) else self._normalize_rule_type(str(rule_value))
        dimension  = str(row.get(dimension_field, "Uniqueness") if dimension_field else "Uniqueness")
        if pd.isna(dimension):
            dimension = "Uniqueness"
        message    = row.get("message")
        message    = str(message) if message and not pd.isna(message) else f"Combination {' + '.join(valid_columns)} should be unique"
        severity   = row.get("severity", "HIGH")
        severity   = str(severity) if severity and not pd.isna(severity) else "HIGH"

        return {
            "rule_type": "uniqueness_combination",
            "columns":   valid_columns,
            "dimension": dimension,
            "message":   message,
            "severity":  severity,
        }

    def _build_single_rule(
        self,
        row: pd.Series,
        column: str,
        rule_field: str,
        dimension_field: Optional[str],
    ) -> Optional[Dict]:
        rule_value = row.get(rule_field)
        if not rule_value or pd.isna(rule_value):
            return None

        rule_type  = self._normalize_rule_type(str(rule_value))
        dimension  = str(row.get(dimension_field, "General") if dimension_field else "General")
        if pd.isna(dimension):
            dimension = "General"
        message    = row.get("message")
        message    = str(message) if message and not pd.isna(message) else f"{column}: {rule_type} validation failed"
        expression = row.get("expression")
        expression = None if pd.isna(expression) or str(expression).lower() == "none" else expression
        severity   = row.get("severity", "MEDIUM")
        severity   = str(severity) if severity and not pd.isna(severity) else "MEDIUM"

        return {
            "column":     column,
            "dimension":  dimension,
            "rule_type":  rule_type,
            "expression": expression,
            "message":    message,
            "severity":   severity,
        }

    def _normalize_rule_type(self, rule_text: str) -> str:
        rule_lower = rule_text.lower().strip()
        if rule_lower in self.rule_alias_map:
            return self.rule_alias_map[rule_lower]
        if "not null" in rule_lower or "not blank" in rule_lower:
            return "not_null"
        if "unique" in rule_lower or "duplicate" in rule_lower:
            return "uniqueness"
        if "email" in rule_lower:
            return "email_format"
        if "regex" in rule_lower or "pattern" in rule_lower:
            return "regex"
        if "numeric" in rule_lower:
            return "numeric_only"
        if "alpha" in rule_lower:
            return "alpha_only"
        if "special char" in rule_lower:
            return "no_special_chars"
        if "date" in rule_lower:
            return "date_format"
        return rule_lower


# ══════════════════════════════════════════════════════════════════════════
#  RULE EXECUTOR ENGINE
# ══════════════════════════════════════════════════════════════════════════

class RuleExecutorEngine:
    """Execute validation rules dynamically with proper column/dimension tracking."""

    def __init__(self, df: pd.DataFrame, rulebook: Dict):
        self.df = df
        self.rulebook = rulebook
        self.duplicate_cache: Dict[str, set] = {}
        self.combination_duplicates: Dict[str, List[List[int]]] = {}
        self._precompute_duplicates()
        self._precompute_combination_duplicates()

    # ── Pre-computation ────────────────────────────────────────────────

    def _precompute_duplicates(self):
        """Pre-compute duplicate row-indices for all columns."""
        for col in self.df.columns:
            value_counts = self.df[col].value_counts()
            dup_vals = value_counts[value_counts > 1].index.tolist()
            dup_indices: set = set()
            for val in dup_vals:
                if not self._is_null_or_empty(val):
                    dup_indices.update(self.df[self.df[col] == val].index.tolist())
            self.duplicate_cache[col] = dup_indices

    def _precompute_combination_duplicates(self):
        """Pre-compute duplicate row-indices for column combinations."""
        for rule in self.rulebook.get("rules", []):
            if rule.get("rule_type") == "uniqueness_combination":
                columns  = rule.get("columns", [])
                if len(columns) < 2:
                    continue
                combo_key = " + ".join(columns)
                combo_vals = self.df[columns].apply(tuple, axis=1)
                value_counts = combo_vals.value_counts()
                dup_groups: List[List[int]] = []
                for val in value_counts[value_counts > 1].index.tolist():
                    if any(self._is_null_or_empty(v) for v in val):
                        continue
                    indices = self.df[combo_vals == val].index.tolist()
                    if len(indices) > 1:
                        dup_groups.append(indices)
                self.combination_duplicates[combo_key] = dup_groups

    def get_combination_duplicates(self) -> Dict[str, List[List[int]]]:
        return self.combination_duplicates

    # ── Main execution ─────────────────────────────────────────────────

    def execute_all_rules(self) -> pd.DataFrame:
        """Execute all rules; return annotated results DataFrame."""
        results = []

        for idx, row in self.df.iterrows():
            row_issues:   List[str]  = []
            row_failed_rules:   List[str]  = []
            row_failed_columns: List[str]  = []
            row_dimensions: set = set()
            row_failed_details: List[Dict] = []

            for rule in self.rulebook.get("rules", []):
                res = self._execute_single_rule(row, rule, idx)
                if not res["passed"]:
                    row_issues.append(res["message"])
                    row_failed_rules.append(res["rule_type"])
                    cols = res.get("columns") or ([res["column"]] if res.get("column") else [])
                    row_failed_columns.extend(cols)
                    row_dimensions.add(res["dimension"])
                    row_failed_details.append({
                        "column":    res.get("column") or " + ".join(res.get("columns", [])),
                        "rule_type": res["rule_type"],
                        "dimension": res["dimension"],
                        "message":   res["message"],
                    })

            result_row = row.to_dict()
            result_row["Issues"]               = " | ".join(row_issues) if row_issues else ""
            result_row["Count of issues"]      = len(row_issues)
            result_row["Failed_Rules"]         = ", ".join(set(row_failed_rules))
            result_row["Failed_Columns"]       = ", ".join(set(row_failed_columns))
            result_row["Issue categories"]     = ", ".join(sorted(row_dimensions))
            result_row["_failed_columns_list"] = list(set(row_failed_columns))
            result_row["_failed_rules_details"]= row_failed_details
            results.append(result_row)

        return pd.DataFrame(results)

    # ── Single rule dispatcher ─────────────────────────────────────────

    def _execute_single_rule(self, row: pd.Series, rule: Dict, row_idx: int) -> Dict:
        rule_type = rule.get("rule_type")
        message   = rule.get("message", "Validation failed")
        dimension = rule.get("dimension", "General")

        if rule_type == "uniqueness_combination":
            return self._execute_combination_uniqueness(row, rule, row_idx)

        column     = rule.get("column")
        expression = rule.get("expression")

        if column not in row.index:
            return {"passed": True, "message": "", "rule_type": rule_type,
                    "column": column, "dimension": dimension}

        value  = row[column]
        passed = True

        try:
            if rule_type == "not_null":
                passed = not self._is_null_or_empty(value)

            elif rule_type == "uniqueness":
                passed = row_idx not in self.duplicate_cache.get(column, set())

            elif rule_type == "regex":
                if not self._is_null_or_empty(value) and expression:
                    passed = bool(re.match(str(expression), str(value)))

            elif rule_type == "allowed_values":
                if not self._is_null_or_empty(value) and expression:
                    allowed = [v.strip() for v in str(expression).split(",")]
                    passed = str(value) in allowed

            elif rule_type == "range":
                if not self._is_null_or_empty(value) and expression:
                    num_val = float(value)
                    mn, mx  = map(float, str(expression).split(","))
                    passed  = mn <= num_val <= mx

            elif rule_type == "length":
                if not self._is_null_or_empty(value) and expression:
                    if "," in str(expression):
                        mn, mx = map(int, str(expression).split(","))
                        passed = mn <= len(str(value)) <= mx
                    else:
                        passed = len(str(value)) == int(expression)

            elif rule_type == "no_special_chars":
                if not self._is_null_or_empty(value):
                    pattern = expression if expression else r'[^A-Za-z0-9\s]'
                    passed  = not bool(re.search(str(pattern), str(value)))

            elif rule_type == "email_format":
                if not self._is_null_or_empty(value):
                    passed = bool(re.match(
                        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
                        str(value),
                    ))

            elif rule_type == "numeric_only":
                if not self._is_null_or_empty(value):
                    float(value)   # raises if non-numeric
                    passed = True

            elif rule_type == "alpha_only":
                if not self._is_null_or_empty(value):
                    passed = str(value).replace(" ", "").isalpha()

            elif rule_type == "date_format":
                if not self._is_null_or_empty(value):
                    fmt = expression if expression else "%Y-%m-%d"
                    datetime.datetime.strptime(str(value), fmt)
                    passed = True

            elif rule_type == "contains":
                if not self._is_null_or_empty(value) and expression:
                    passed = str(expression) in str(value)

            elif rule_type == "not_contains":
                if not self._is_null_or_empty(value) and expression:
                    passed = str(expression) not in str(value)

            elif rule_type == "custom_expression":
                if expression:
                    passed = self._evaluate_safe_expression(value, expression)

        except Exception as e:
            passed  = False
            message = f"{message} (Error: {str(e)})"

        return {
            "passed":    passed,
            "message":   message if not passed else "",
            "rule_type": rule_type,
            "column":    column,
            "dimension": dimension,
        }

    def _execute_combination_uniqueness(
        self, row: pd.Series, rule: Dict, row_idx: int
    ) -> Dict:
        columns   = rule.get("columns", [])
        message   = rule.get("message", "Duplicate combination found")
        dimension = rule.get("dimension", "Uniqueness")

        if not columns or len(columns) < 2:
            return {"passed": True, "message": "", "rule_type": "uniqueness_combination",
                    "columns": columns, "dimension": dimension}

        combo_key = " + ".join(columns)
        dup_groups = self.combination_duplicates.get(combo_key, [])
        passed = all(row_idx not in grp for grp in dup_groups)

        return {
            "passed":    passed,
            "message":   message if not passed else "",
            "rule_type": "uniqueness_combination",
            "columns":   columns,
            "column":    combo_key,
            "dimension": dimension,
        }

    # ── Helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _is_null_or_empty(value) -> bool:
        return (
            value is None
            or (isinstance(value, float) and pd.isna(value))
            or str(value).strip() == ""
            or str(value).lower() == "nan"
        )

    @staticmethod
    def _evaluate_safe_expression(value, expression: str) -> bool:
        try:
            safe_builtins = {"__builtins__": {}}
            safe_vars = {
                "value": value, "len": len, "str": str,
                "int": int, "float": float, "abs": abs,
                "min": min, "max": max,
            }
            if any(kw in expression for kw in ["import", "exec", "eval", "__", "open", "file"]):
                return False
            return bool(eval(expression, safe_builtins, safe_vars))
        except Exception:
            return False


# ══════════════════════════════════════════════════════════════════════════
#  DATA QUALITY ENGINE  (high-level orchestrator)
# ══════════════════════════════════════════════════════════════════════════

class DataQualityEngine:
    """High-level DQ orchestrator used by app.py."""

    def __init__(self, df: pd.DataFrame, rules_path: Path):
        self.df         = df
        self.rules_path = Path(rules_path)
        self.rulebook   = self._load_rulebook()
        self.results_df = None

    def _load_rulebook(self) -> Dict:
        builder = RulebookBuilderService()
        if self.rules_path.suffix.lower() == ".json":
            return builder.load_json_rulebook(self.rules_path)
        # Import here to avoid circular; data_io_core lives in same package
        from modules.data_io_core import FileLoaderService
        loader   = FileLoaderService()
        rules_df = loader.load_dataframe(self.rules_path)
        return builder.build_from_rules_dataset(rules_df, self.df.columns.tolist())

    def run(self) -> Dict[str, Any]:
        executor        = RuleExecutorEngine(self.df, self.rulebook)
        self.results_df = executor.execute_all_rules()

        from modules.reporting_core import ScoringService
        overall_score     = ScoringService.calculate_overall_score(self.results_df)
        dimension_scores  = ScoringService.calculate_dimension_scores(self.results_df)
        column_scores     = ScoringService.calculate_column_scores(
            self.results_df, self.df.columns.tolist()
        )
        clean_records = len(self.results_df[self.results_df["Count of issues"] == 0])

        return {
            "overall_score":    overall_score,
            "total_records":    len(self.df),
            "clean_records":    clean_records,
            "total_issues":     int(self.results_df["Count of issues"].sum()),
            "dimension_scores": dimension_scores,
            "column_scores":    column_scores,
            "results_df":       self.results_df,
            "rulebook":         self.rulebook,
            "duplicate_combinations": executor.get_combination_duplicates(),
        }
