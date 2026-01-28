from __future__ import annotations
import pathlib
import pandas as pd

def list_files(folder: str, allowed_exts: list[str]) -> list[pathlib.Path]:
    p = pathlib.Path(folder)
    exts = set(["." + e.lstrip(".").lower() for e in allowed_exts])
    files = []
    for f in sorted(p.glob("*")):
        if not f.is_file():
            continue
        if f.name.startswith("~$") or f.name.startswith("."):
            continue
        if f.suffix.lower() in exts:
            files.append(f)
    return files

def read_table(path: pathlib.Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in (".xlsx", ".xlsm", ".xltx", ".xltm"):
        return pd.read_excel(path, dtype=object, engine="openpyxl")
    if suffix in (".xls", ".xlsb"):
        raise ValueError(f"Unsupported Excel format {suffix} for {path.name}. Please convert to .xlsx or .csv.")
    return pd.read_csv(path, dtype=object)
