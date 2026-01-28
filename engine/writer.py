from __future__ import annotations
import pathlib
import pandas as pd

def write_table(df: pd.DataFrame, out_path: pathlib.Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.suffix.lower() in (".xlsx", ".xlsm", ".xltx", ".xltm"):
        df.to_excel(out_path, index=False)
    else:
        df.to_csv(out_path, index=False, encoding="utf-8")
