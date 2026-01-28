from __future__ import annotations
import pathlib
import re
import pandas as pd
from .models import JobConfig

try:
    import openpyxl
except ImportError:
    openpyxl = None

def _clean(s: str) -> str:
    s = str(s).replace("\r\n", "\n").replace("\r", "\n")
    return s.strip()

def _extract_from_excel_cell(source_path: pathlib.Path, cell: str) -> str | None:
    suffix = source_path.suffix.lower()
    if suffix in (".xls", ".xlsb"):
        raise RuntimeError(f"Unsupported Excel format {suffix} for {source_path.name}. Please convert to .xlsx or .csv.")
    if openpyxl is None:
        raise RuntimeError("openpyxl missing. Install openpyxl.")
    wb = openpyxl.load_workbook(source_path, data_only=True)
    ws = wb.active
    v = ws[cell].value
    if v is None or str(v).strip() == "":
        return None
    return _clean(v)

def _extract_from_csv_row(source_path: pathlib.Path, cfg) -> str | None:
    df = pd.read_csv(source_path, dtype=object)
    col = cfg.csv.row_match_column
    if col not in df.columns:
        return None

    target_val = str(cfg.csv.row_match_equals).strip().lower()
    series = df[col].astype(str).str.strip().str.lower()
    hits = df[series == target_val]
    if hits.empty:
        return None

    row = hits.iloc[0]
    for pref in cfg.csv.value_column_preference:
        if pref.startswith("col_index:"):
            idx = int(pref.split(":", 1)[1])
            if idx < len(row):
                v = row.iloc[idx]
                if v is not None and str(v).strip() != "":
                    return _clean(v)
        else:
            if pref in df.columns:
                v = row.get(pref, None)
                if v is not None and str(v).strip() != "":
                    return _clean(v)
    return None

def _extract_from_filename_regex(source_path: pathlib.Path, pattern: str) -> str | None:
    name = source_path.name
    stem = source_path.stem
    for text in (stem, name):
        m = re.search(pattern, text)
        if m:
            if "customer" in m.groupdict():
                return _clean(m.group("customer"))
            if m.groups():
                return _clean(m.group(1))
    return None

def extract_customer_key(source_path: pathlib.Path, job: JobConfig, row: pd.Series | None = None) -> str | None:
    cfg = job.customer_match.source
    t = cfg.type

    if t == "excel_cell_or_csv_row":
        if source_path.suffix.lower() in (".xlsx", ".xlsm", ".xltx", ".xltm"):
            return _extract_from_excel_cell(source_path, cfg.excel_cell or job.source.customer.excel_cell)
        return _extract_from_csv_row(source_path, job.source.customer)

    if t == "excel_cell":
        return _extract_from_excel_cell(source_path, cfg.excel_cell or job.source.customer.excel_cell)

    if t == "csv_row":
        return _extract_from_csv_row(source_path, job.source.customer)

    if t == "filename_regex":
        if not cfg.filename_regex:
            raise ValueError("customer_match.source.filename_regex missing")
        return _extract_from_filename_regex(source_path, cfg.filename_regex)

    if t == "filename":
        return _clean(source_path.stem)

    if t == "column":
        if row is None:
            return None
        col = cfg.column
        if not col:
            return None
        resolved = _resolve_column_from_columns(row.index, col) or col
        if resolved not in row.index:
            return None
        v = row.get(resolved, None)
        if v is None:
            return None
        sv = str(v).strip()
        if sv == "" or sv.lower() == "nan":
            return None
        return _clean(sv)

    raise ValueError(f"Unsupported customer source type: {t}")

def _join_column(df: pd.DataFrame, col: str) -> str | None:
    if col not in df.columns:
        return None
    parts = []
    for x in df[col].tolist():
        if x is None:
            continue
        sx = str(x).strip()
        if sx == "" or sx.lower() == "nan":
            continue
        parts.append(_clean(sx))
    if not parts:
        return None
    return "\n".join(parts)

def _col_index_from_letter(col_letter: str) -> int:
    col_letter = col_letter.strip().upper()
    if not col_letter.isalpha():
        raise ValueError(f"Invalid column letter: {col_letter}")
    idx = 0
    for ch in col_letter:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1

def _resolve_column_from_columns(columns, col_spec: str) -> str | None:
    if col_spec.startswith("col_index:"):
        idx = int(col_spec.split(":", 1)[1])
        if 0 <= idx < len(columns):
            return columns[idx]
        return None
    if col_spec.startswith("col_letter:"):
        idx = _col_index_from_letter(col_spec.split(":", 1)[1])
        if 0 <= idx < len(columns):
            return columns[idx]
        return None
    if col_spec in columns:
        return col_spec
    return None

def _join_columns(df: pd.DataFrame, columns: list[str] | str) -> str | None:
    if isinstance(columns, str):
        columns = [columns]
    for col in columns:
        resolved = _resolve_column_from_columns(df.columns, col)
        if resolved is None:
            continue
        text = _join_column(df, resolved)
        if text is not None:
            return text
    return None

def _row_value_first(row: pd.Series, columns: list[str] | str) -> str | None:
    if isinstance(columns, str):
        columns = [columns]
    for col in columns:
        resolved = _resolve_column_from_columns(row.index, col)
        if resolved is None:
            continue
        v = row.get(resolved, None)
        if v is None:
            continue
        sv = str(v).strip()
        if sv == "" or sv.lower() == "nan":
            continue
        return _clean(sv)
    return None

def extract_texts(source_path: pathlib.Path, job: JobConfig) -> tuple[str | None, str | None]:
    from .source_reader import read_table
    df = read_table(source_path)

    cfg = job.source.content
    if cfg.mode != "join_column":
        raise ValueError("Only join_column supported for extract_texts.")

    if "Element" in df.columns:
        series = df["Element"].astype(str).str.strip().str.lower()
        df = df[series != "element"]

    texts = {}
    for lang, lcfg in cfg.languages.items():
        texts[lang] = _join_columns(df, lcfg.columns)
    text_de = texts.get("DE")
    text_fr = texts.get("FR")
    return text_de, text_fr

def extract_row_items(source_path: pathlib.Path, job: JobConfig) -> list[tuple[str, dict[str, str | None], pd.Series]]:
    from .source_reader import read_table
    df = read_table(source_path)

    element_col_spec = job.source.element.column
    element_col = _resolve_column_from_columns(df.columns, element_col_spec) or element_col_spec
    if element_col not in df.columns:
        raise ValueError(f"Source missing element column: {element_col_spec}")

    cfg = job.source.content
    if cfg.mode != "row_columns":
        raise ValueError("Only row_columns supported for per-row mapping.")

    rows: list[tuple[str, dict[str, str | None], pd.Series]] = []
    for _, row in df.iterrows():
        elem_raw = row.get(element_col, None)
        if elem_raw is None:
            continue
        elem = str(elem_raw).strip()
        if elem == "" or elem.lower() == "element":
            continue

        texts: dict[str, str | None] = {}
        for lang, lcfg in cfg.languages.items():
            texts[lang] = _row_value_first(row, lcfg.columns)

        if all(v is None for v in texts.values()):
            rows.append((elem, texts, row))
            continue

        rows.append((elem, texts, row))
    return rows
