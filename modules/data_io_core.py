"""
modules/data_io_core.py
══════════════════════════════════════════════════════════════════════════
Merged module: file_loader + utils

Contains:
  • FileLoaderService    — universal file loading (CSV/Excel/JSON/Parquet…)
  • LegacyFileLoader     — backward-compatibility shim
  • Utility functions   — directory management, file saving, value cleaning
══════════════════════════════════════════════════════════════════════════
"""

import os
import gc
import json
import shutil
import time
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional, List

import pandas as pd
import numpy as np

from modules.config import AppConfig


# ══════════════════════════════════════════════════════════════════════════
#  FILE LOADER SERVICE
# ══════════════════════════════════════════════════════════════════════════

class FileLoaderService:
    """Universal loader supporting CSV, TSV, Excel variants, JSON, Parquet, ODS, XML."""

    SUPPORTED = [
        ".csv", ".tsv",
        ".xlsx", ".xls", ".xlsm",
        ".xlsb",   # loaded via pyxlsb — MIME-safe
        ".ods",
        ".json",
        ".parquet",
        ".xml",
    ]

    def __init__(self):
        self.supported_formats = self.SUPPORTED

    # ── Public API ────────────────────────────────────────────────────────

    def load_dataframe(
        self,
        file_path: Path,
        sheet_name: Optional[str] = None,
        columns:    Optional[List[str]] = None,
    ) -> pd.DataFrame:
        file_path = Path(file_path)
        ext = file_path.suffix.lower()
        if ext not in self.SUPPORTED:
            raise ValueError(f"Unsupported format: '{ext}'. Supported: {', '.join(self.SUPPORTED)}")

        try:
            if ext in (".csv", ".tsv"):
                df = self._load_csv(file_path, ext)
            elif ext in (".xlsx", ".xls", ".xlsm"):
                df = self._load_excel_openpyxl(file_path, sheet_name)
            elif ext == ".xlsb":
                df = self._load_xlsb(file_path, sheet_name)
            elif ext == ".ods":
                df = self._load_ods(file_path, sheet_name)
            elif ext == ".json":
                df = self._load_json(file_path)
            elif ext == ".parquet":
                df = self._load_parquet(file_path)
            elif ext == ".xml":
                df = self._load_xml(file_path)
            else:
                raise ValueError(f"Unhandled type: {ext}")

            df = self._normalize_dataframe(df)
            if columns:
                missing = [c for c in columns if c not in df.columns]
                if missing:
                    raise ValueError(f"Columns not found: {missing}")
                df = df[columns]
            return df
        except Exception as e:
            raise Exception(f"Error loading '{file_path.name}': {str(e)}")

    def get_sheet_names(self, file_path: Path) -> List[str]:
        file_path = Path(file_path)
        ext = file_path.suffix.lower()
        if ext not in (".xlsx", ".xls", ".xlsm", ".xlsb", ".ods"):
            return []
        try:
            time.sleep(0.1)
            if ext == ".xlsb":
                return self._get_xlsb_sheet_names(file_path)
            if ext == ".ods":
                return pd.ExcelFile(file_path, engine="odf").sheet_names
            return pd.ExcelFile(file_path, engine="openpyxl").sheet_names
        except (EOFError, zipfile.BadZipFile):
            raise ValueError("Excel file is corrupted or incomplete.")
        except Exception as e:
            raise ValueError(f"Cannot read file: {str(e)}")

    def validate_file(self, file_path: Path) -> bool:
        try:
            file_path = Path(file_path)
            ext = file_path.suffix.lower()
            if ext not in self.SUPPORTED or file_path.stat().st_size == 0:
                return False
            if ext in (".csv", ".tsv"):
                pd.read_csv(file_path, sep="\t" if ext == ".tsv" else ",", nrows=1)
            elif ext in (".xlsx", ".xls", ".xlsm"):
                pd.read_excel(file_path, nrows=1, engine="openpyxl")
            elif ext == ".xlsb":
                self._load_xlsb(file_path, nrows=1)
            elif ext == ".ods":
                pd.read_excel(file_path, nrows=1, engine="odf")
            elif ext == ".json":
                with open(file_path) as f:
                    json.load(f)
            elif ext == ".parquet":
                pd.read_parquet(file_path).head(1)
            elif ext == ".xml":
                pd.read_xml(file_path).head(1)
            return True
        except Exception:
            return False

    # ── Private loaders ───────────────────────────────────────────────────

    def _load_csv(self, file_path: Path, ext: str) -> pd.DataFrame:
        sep = "\t" if ext == ".tsv" else ","
        try:
            return pd.read_csv(file_path, sep=sep, dtype=str, low_memory=False, encoding="utf-8")
        except UnicodeDecodeError:
            return pd.read_csv(file_path, sep=sep, dtype=str, low_memory=False, encoding="latin-1")

    def _load_excel_openpyxl(
        self, file_path: Path, sheet_name: Optional[str], nrows: Optional[int] = None
    ) -> pd.DataFrame:
        max_retries, delay = 3, 0.2
        for attempt in range(max_retries):
            try:
                if file_path.stat().st_size == 0:
                    raise ValueError("Excel file is empty.")
                if attempt == 0:
                    time.sleep(0.1)
                xls   = pd.ExcelFile(file_path, engine="openpyxl")
                sheet = sheet_name if (sheet_name and sheet_name in xls.sheet_names) else xls.sheet_names[0]
                kwargs = {"sheet_name": sheet, "dtype": str}
                if nrows is not None:
                    kwargs["nrows"] = nrows
                return pd.read_excel(xls, **kwargs)
            except (EOFError, zipfile.BadZipFile):
                if attempt < max_retries - 1:
                    time.sleep(delay); delay *= 2; continue
                raise ValueError("Excel file appears corrupted or incomplete.")

    def _load_xlsb(
        self, file_path: Path, sheet_name: Optional[str] = None, nrows: Optional[int] = None
    ) -> pd.DataFrame:
        try:
            import pyxlsb
        except ImportError:
            raise ImportError("pyxlsb is required for .xlsb files: pip install pyxlsb")

        with pyxlsb.open_workbook(str(file_path)) as wb:
            sheets = wb.sheets
            if not sheets:
                raise ValueError("No sheets found in .xlsb workbook.")
            target = sheet_name if (sheet_name and sheet_name in sheets) else sheets[0]
            rows_data: List[list] = []
            with wb.get_sheet(target) as sheet:
                for row_idx, row in enumerate(sheet.rows()):
                    rows_data.append([cell.v for cell in row])
                    if nrows is not None and row_idx >= nrows:
                        break

        if not rows_data:
            return pd.DataFrame()
        headers = [str(h).strip() if h is not None else f"Column_{i}" for i, h in enumerate(rows_data[0])]
        df = pd.DataFrame(rows_data[1:], columns=headers)
        df = df.applymap(lambda x: str(x) if x is not None else None)
        return df

    def _get_xlsb_sheet_names(self, file_path: Path) -> List[str]:
        try:
            import pyxlsb
        except ImportError:
            raise ImportError("pyxlsb is required for .xlsb files: pip install pyxlsb")
        with pyxlsb.open_workbook(str(file_path)) as wb:
            return list(wb.sheets)

    def _load_ods(self, file_path: Path, sheet_name: Optional[str] = None) -> pd.DataFrame:
        try:
            import odf  # noqa: F401
        except ImportError:
            raise ImportError("odfpy is required for .ods files: pip install odfpy")
        xls   = pd.ExcelFile(file_path, engine="odf")
        sheet = sheet_name if (sheet_name and sheet_name in xls.sheet_names) else xls.sheet_names[0]
        return pd.read_excel(xls, sheet_name=sheet, dtype=str)

    def _load_json(self, file_path: Path) -> pd.DataFrame:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return pd.DataFrame(data)
        if isinstance(data, dict):
            return pd.DataFrame(data.get("data", [data]))
        raise ValueError("JSON must be a list or dict.")

    def _load_parquet(self, file_path: Path) -> pd.DataFrame:
        try:
            return pd.read_parquet(file_path).astype(str)
        except ImportError:
            raise ImportError("pyarrow is required for .parquet files: pip install pyarrow")

    def _load_xml(self, file_path: Path) -> pd.DataFrame:
        try:
            return pd.read_xml(file_path, dtype=str)
        except Exception as e:
            raise ValueError(f"Could not parse XML: {str(e)}")

    @staticmethod
    def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.str.strip()
        return df.where(pd.notnull(df), None)


# ── Legacy shim ───────────────────────────────────────────────────────────

class LegacyFileLoader:
    """Backward-compatible wrapper around FileLoaderService."""

    @staticmethod
    def load_data(path: str, sheet: Optional[str] = None, columns: Optional[List[str]] = None) -> pd.DataFrame:
        return FileLoaderService().load_dataframe(Path(path), sheet, columns)

    @staticmethod
    def load_csv_data(path: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
        return LegacyFileLoader.load_data(path=path, columns=columns)

    @staticmethod
    def load_excel_data(path: str, sheet: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
        return LegacyFileLoader.load_data(path=path, sheet=sheet, columns=columns)


# ══════════════════════════════════════════════════════════════════════════
#  UTILITY FUNCTIONS  (from utils.py)
# ══════════════════════════════════════════════════════════════════════════

def setup_directories():
    """Create necessary application directories."""
    AppConfig.TEMP_DIR.mkdir(exist_ok=True)
    AppConfig.OUTPUT_DIR.mkdir(exist_ok=True)
    if hasattr(AppConfig, "RULES_DIR"):
        AppConfig.RULES_DIR.mkdir(exist_ok=True)


def clean_temp_directory():
    """Clean temporary directory with robust error handling."""
    if not AppConfig.TEMP_DIR.exists():
        AppConfig.TEMP_DIR.mkdir(exist_ok=True)
        return
    try:
        gc.collect()
        shutil.rmtree(AppConfig.TEMP_DIR)
        AppConfig.TEMP_DIR.mkdir(exist_ok=True)
    except PermissionError:
        for file_path in AppConfig.TEMP_DIR.iterdir():
            try:
                if file_path.is_file():
                    try:
                        file_path.unlink()
                    except PermissionError:
                        time.sleep(0.5)
                        file_path.unlink()
                elif file_path.is_dir():
                    shutil.rmtree(file_path)
            except (PermissionError, OSError):
                pass
        AppConfig.TEMP_DIR.mkdir(exist_ok=True)
    except Exception:
        AppConfig.TEMP_DIR.mkdir(exist_ok=True)


def clean_temp_directory_safe(max_retries: int = 3) -> bool:
    """Enhanced temp directory cleaning with retry logic."""
    for attempt in range(max_retries):
        try:
            gc.collect()
            if AppConfig.TEMP_DIR.exists():
                shutil.rmtree(AppConfig.TEMP_DIR)
            AppConfig.TEMP_DIR.mkdir(exist_ok=True)
            return True
        except (PermissionError, OSError):
            if attempt < max_retries - 1:
                time.sleep(0.5)
            else:
                AppConfig.TEMP_DIR.mkdir(exist_ok=True)
                return False
    return False


def save_uploaded_file(uploaded_file, directory: Path) -> Path:
    """Save a Streamlit UploadedFile to disk."""
    file_path = Path(directory) / uploaded_file.name
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return file_path


def get_timestamp() -> str:
    """Return current timestamp string suitable for filenames."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def format_file_size(size_bytes: int) -> str:
    """Human-readable file size."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


def clean_value(value):
    """Clean a value for Excel output — handles all edge cases."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple, np.ndarray)):
        return ", ".join(str(v) for v in value) if len(value) else ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(value, float):
        try:
            if np.isnan(value):
                return ""
        except (TypeError, ValueError):
            pass
    str_value = str(value).strip()
    return "" if str_value.lower() == "nan" else str(value)


def is_null_or_empty(value) -> bool:
    """Check if a value is null or empty."""
    if value is None:
        return True
    if isinstance(value, (list, tuple, np.ndarray)):
        return len(value) == 0
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return str(value).strip() in ("", "nan")
