"""
Configuration module for Enterprise DQ Engine
Centralized configuration management
"""

from pathlib import Path
from enum import Enum
from typing import List, Dict


class AppConfig:
    """Main application configuration"""
    
    # Application metadata
    APP_TITLE = "Enterprise Data Quality Rule Engine"
    APP_ICON = "📊"
    VERSION = "2.0.0"
    
    # Directory configuration
    BASE_DIR = Path(__file__).parent.parent
    TEMP_DIR = BASE_DIR / "temp"
    OUTPUT_DIR = BASE_DIR / "output"
    RULES_DIR = BASE_DIR / "rules"
    
    # File format support
    SUPPORTED_DATA_FORMATS = ["csv", "xlsx", "xlsm", "xls", "xlsb", "tsv", "json", "parquet", "ods", "xml"]
    SUPPORTED_RULES_FORMATS = ["csv", "xlsx", "xls"]
    
    # Validation configuration
    MAX_FILE_SIZE_MB = 200
    MAX_ROWS_PREVIEW = 100
    
    # Rule types
    RULE_TYPES = [
        "not_null",
        "uniqueness",
        "regex",
        "allowed_values",
        "range",
        "length",
        "no_special_chars",
        "email_format",
        "numeric_only",
        "alpha_only",
        "date_format",
        "contains",
        "not_contains",
        "custom_expression",
        "phone_format",
        "url_format"
    ]
    
    # DQ Dimensions
    DIMENSIONS = [
        "Completeness",
        "Validity",
        "Uniqueness",
        "Standardization",
        "Accuracy",
        "Consistency",
        "Timeliness"
    ]
    
    # Severity levels
    SEVERITY_LEVELS = ["HIGH", "MEDIUM", "LOW"]
    
    # Excel formatting
    EXCEL_HEADER_COLOR = "#4472C4"
    EXCEL_PASS_COLOR = "#C6EFCE"
    EXCEL_FAIL_COLOR = "#FFC7CE"
    
    # Score thresholds
    SCORE_EXCELLENT = 95
    SCORE_GOOD = 80
    SCORE_FAIR = 60


class MasterDataType(Enum):
    """Master data type enumeration"""
    USER = "user"
    CUSTOMER = "customer"
    VENDOR = "vendor"
    MATERIAL = "material"
    CENTER = "center"
    CUSTOM = "custom"


class RuleType(Enum):
    """Rule type enumeration"""
    NOT_NULL = "not_null"
    UNIQUENESS = "uniqueness"
    REGEX = "regex"
    ALLOWED_VALUES = "allowed_values"
    RANGE = "range"
    LENGTH = "length"
    NO_SPECIAL_CHARS = "no_special_chars"
    EMAIL_FORMAT = "email_format"
    NUMERIC_ONLY = "numeric_only"
    ALPHA_ONLY = "alpha_only"
    DATE_FORMAT = "date_format"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    CUSTOM_EXPRESSION = "custom_expression"


# Rule alias mappings (for backward compatibility)
RULE_ALIAS_MAP = {
    # Completeness
    "should not be null or blank": "not_null",
    "not null": "not_null",
    "mandatory": "not_null",
    "required": "not_null",
    
    # Validity
    "map city aliases": "standardization",
    "city should be valid": "standardization",
    "map country variants": "standardization",
    "country should be valid": "standardization",
    "valid email": "email_format",
    "email validation": "email_format",
    
    # Regex
    "validate format": "regex",
    "format check": "regex",
    "pattern match": "regex",
    
    # Standardization
    "trim spaces": "no_special_chars",
    "normalize spacing": "no_special_chars",
    "remove special characters": "no_special_chars",
    
    # Uniqueness
    "unique": "uniqueness",
    "no duplicates": "uniqueness",
    "check duplicates": "uniqueness"
}


# Default column mappings (for backward compatibility with existing system)
COLUMN_MAPPINGS: Dict[str, List[str]] = {
    "user": [],
    "customer": [],
    "customer_address": [],
    "vendor": [],
    "vendor_address": [],
    "material_basic": [],
    "material_sales": [],
    "material_plant": [],
    "center": []
}


# Default sheets per master type
DEFAULT_SHEETS: Dict[str, List[str]] = {
    "user": ["user"],
    "customer": ["customer", "customer_address"],
    "vendor": ["vendor", "vendor_address"],
    "material": ["material_basic", "material_sales", "material_plant"],
    "center": ["center"]
}


# Material types
MATERIAL_TYPES: List[str] = [
    "RAW_MATERIAL",
    "FINISHED_GOOD",
    "SEMI_FINISHED",
    "PACKAGING",
    "SERVICE"
]
