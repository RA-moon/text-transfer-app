from __future__ import annotations
import re
import pandas as pd

def normalize_text(x, steps: list[str]) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x)
    for step in steps:
        if step == "strip":
            s = s.strip()
        elif step == "lower":
            s = s.lower()
        elif step == "collapse_spaces":
            s = re.sub(r"\s+", " ", s).strip()
        else:
            raise ValueError(f"Unknown normalize step: {step}")
    return s
