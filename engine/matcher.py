from __future__ import annotations
import pandas as pd
import re
from difflib import SequenceMatcher
from .normalize import normalize_text
from .models import JobConfig

def _resolve_column_from_columns(columns, col_spec: str) -> str | None:
    if col_spec.startswith("col_index:"):
        idx = int(col_spec.split(":", 1)[1])
        if 0 <= idx < len(columns):
            return columns[idx]
        return None
    if col_spec.startswith("col_letter:"):
        col_letter = col_spec.split(":", 1)[1].strip().upper()
        if not col_letter.isalpha():
            return None
        idx = 0
        for ch in col_letter:
            idx = idx * 26 + (ord(ch) - ord("A") + 1)
        idx -= 1
        if 0 <= idx < len(columns):
            return columns[idx]
        return None
    if col_spec in columns:
        return col_spec
    return None

def _extract_filename_key(path, pattern: str) -> str | None:
    name = path.name
    stem = path.stem
    for text in (name, stem):
        m = re.search(pattern, text)
        if m:
            if "customer" in m.groupdict():
                return m.group("customer")
            if m.groups():
                return m.group(1)
    return None

def find_matches(df_target: pd.DataFrame, customer_key: str | None, job: JobConfig, target_path) -> pd.Series:
    if customer_key is None:
        return pd.Series([False] * len(df_target))

    steps = job.customer_match.normalize or []
    key = normalize_text(customer_key, steps)
    mode = (job.customer_match.mode or "exact").lower()
    tgt_cfg = job.customer_match.target
    t = tgt_cfg.type

    if t == "column":
        col = tgt_cfg.column or job.target.match.column
        if col is None:
            raise ValueError("Target match column missing")
        resolved = _resolve_column_from_columns(df_target.columns, col) or col
        if resolved not in df_target.columns:
            raise ValueError(f"Target missing column: {resolved}")
        labels = df_target[resolved].map(lambda x: normalize_text(x, steps))
        if mode == "contains":
            return labels.map(lambda x: key in x if isinstance(x, str) else False)
        if mode == "fuzzy":
            threshold = job.customer_match.fuzzy_threshold
            if threshold is None:
                threshold = 0.78
            best_idx = None
            best_score = 0.0
            for idx, val in labels.items():
                if not val:
                    continue
                score = SequenceMatcher(None, key, val).ratio()
                if score > best_score:
                    best_score = score
                    best_idx = idx
            mask = labels.map(lambda _: False)
            if best_idx is not None and best_score >= threshold:
                mask.loc[best_idx] = True
            return mask
        return labels == key

    if t == "filename_regex":
        if not tgt_cfg.filename_regex:
            raise ValueError("customer_match.target.filename_regex missing")
        target_key = _extract_filename_key(target_path, tgt_cfg.filename_regex)
        if target_key is None:
            return pd.Series([False] * len(df_target))
        target_norm = normalize_text(target_key, steps)
        if mode == "contains":
            return pd.Series([key in target_norm] * len(df_target))
        return pd.Series([target_norm == key] * len(df_target))

    if t == "filename":
        target_key = target_path.stem
        target_norm = normalize_text(target_key, steps)
        if mode == "contains":
            return pd.Series([key in target_norm] * len(df_target))
        return pd.Series([target_norm == key] * len(df_target))

    raise ValueError(f"Unsupported target match type: {t}")
