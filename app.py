from __future__ import annotations
import json
import pathlib
import shutil
import yaml
import pandas as pd

from engine.source_reader import list_files, read_table
from engine.extractors import extract_row_items, extract_customer_key
import engine.runner as runner_mod
import streamlit as st

from engine.job_loader import load_job, load_job_from_raw, JobConfigError
from engine.runner import dry_run, run

APP_NAME = "RA-moon's List-Wizard"
APP_TITLE = "RA-moon's List-Wizard (Local Only)"
README_PATH = pathlib.Path("README.md").resolve()
BASE_DIR = pathlib.Path.home() / "Desktop" / APP_NAME
CONFIG_DIR = BASE_DIR / "configs"
TEMPLATE_DIR = CONFIG_DIR / "templates"
DEFAULT_SOURCE_DIR = BASE_DIR / "input" / "datensammlungen"
DEFAULT_TARGET_DIR = BASE_DIR / "input" / "zieldateien"
DEFAULT_OUTPUT_DIR = BASE_DIR / "output"

def ensure_app_dirs():
    (BASE_DIR / "input" / "datensammlungen").mkdir(parents=True, exist_ok=True)
    (BASE_DIR / "input" / "zieldateien").mkdir(parents=True, exist_ok=True)
    (BASE_DIR / "output").mkdir(parents=True, exist_ok=True)
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)

def seed_default_job():
    existing = list(CONFIG_DIR.glob("*.yml")) + list(CONFIG_DIR.glob("*.yaml")) + list(CONFIG_DIR.glob("*.json"))
    if existing:
        return
    src = pathlib.Path("jobs/customer_texts.yml")
    if src.exists():
        shutil.copy2(src, CONFIG_DIR / src.name)

def list_job_files(config_dir: pathlib.Path):
    if not config_dir.exists():
        return []
    return sorted([p for p in config_dir.glob("*.yml")] + [p for p in config_dir.glob("*.yaml")] + [p for p in config_dir.glob("*.json")])

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)
st.caption("Lokale Verarbeitung. Keine Netzwerkfunktionen. Originaldateien werden nie überschrieben.")

ensure_app_dirs()
seed_default_job()

job_files = list_job_files(CONFIG_DIR)
if not job_files:
    st.error("Keine Job-Dateien im Config-Ordner gefunden.")
    st.stop()

with st.sidebar:
    st.subheader("Run Settings")
    st.caption(f"Arbeitsordner: {BASE_DIR}")
    st.caption(f"Configs: {CONFIG_DIR}")
    job_name = st.selectbox(
        "Job auswählen",
        [p.name for p in job_files],
        key="job_select",
        help="Welche Config fuer den Run verwendet wird.",
    )
    source_dir = st.text_input(
        "Source-Ordner (Datensammlungen)",
        value=str(DEFAULT_SOURCE_DIR),
        key="source_dir",
        help="Ordner mit den Eingabedateien (xlsx/csv).",
    )
    target_dir = st.text_input(
        "Target-Ordner (Zieldateien)",
        value=str(DEFAULT_TARGET_DIR),
        key="target_dir",
        help="Ordner mit den Ziel-Dateien (xlsx/csv).",
    )
    output_dir = st.text_input(
        "Output-Basisordner",
        value=str(DEFAULT_OUTPUT_DIR),
        key="output_dir",
        help="Hier werden Run-Ausgaben erzeugt (report, blocked, updated_targets, etc.).",
    )
    st.divider()
    st.caption("Run-Optionen")
    strict_override = st.checkbox(
        "Strict single match",
        value=False,
        key="strict_override",
        help="Blockiert wenn mehr als ein Ziel-Row matched.",
    )
    write_reports = st.checkbox(
        "Lokale Reports",
        value=True,
        key="write_reports",
        help="Erzeugt report.csv, blocked.csv und audit.json.",
    )
    write_collisions = st.checkbox(
        "Collision-Log",
        value=True,
        key="write_collisions",
        help="Erzeugt collisions.csv fuer widerspruechliche Writes.",
    )

job_path = CONFIG_DIR / job_name

def dir_ok(p: str) -> bool:
    pp = pathlib.Path(p)
    return pp.exists() and pp.is_dir()

job = None
try:
    job = load_job(job_path)
except JobConfigError as e:
    st.error(f"Job-Config ungültig: {e}")

def _load_editor_raw():
    try:
        return yaml.safe_load(st.session_state.get("editor_content", ""))
    except Exception:
        return None

def _dump_config(raw: dict, filename: str) -> str:
    if filename.endswith(".json"):
        return json.dumps(raw, indent=2, ensure_ascii=True)
    return yaml.safe_dump(raw, sort_keys=False, allow_unicode=False)

def _split_csv_list(value: str) -> list[str]:
    parts = [p.strip() for p in str(value or "").split(",")]
    return [p for p in parts if p]

def _resolve_column(columns, col_spec: str) -> str | None:
    if col_spec.startswith("col_index:"):
        try:
            idx = int(col_spec.split(":", 1)[1])
        except ValueError:
            return None
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
    return col_spec if col_spec in columns else None

def _find_latest_social_unmapped(output_dir: str) -> pathlib.Path | None:
    base = pathlib.Path(output_dir)
    if not base.exists() or not base.is_dir():
        return None
    run_dirs = [p for p in base.glob("run_*") if p.is_dir()]
    run_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    for run_dir in run_dirs:
        candidate = run_dir / "social_unmapped.csv"
        if candidate.exists():
            return candidate
    return None

def _load_job_from_editor():
    try:
        raw = yaml.safe_load(st.session_state.get("editor_content", ""))
        return load_job_from_raw(raw)
    except Exception:
        return None

def _validate_editor_content():
    try:
        raw = yaml.safe_load(st.session_state.get("editor_content", ""))
        load_job_from_raw(raw)
        return True, ""
    except Exception as e:
        return False, str(e)

def _get_first_columns(folder: str, file_types: list[str]) -> list[str]:
    try:
        paths = list_files(folder, file_types)
        if not paths:
            return []
        df = read_table(paths[0])
        return [str(c) for c in df.columns]
    except Exception:
        return []

def _select_with_custom(label: str, options: list[str], current: str, key_prefix: str, help_text: str = "") -> str:
    if options:
        opts = options + ["<custom>"]
        default_idx = opts.index(current) if current in options else opts.index("<custom>")
        choice = st.selectbox(label, opts, index=default_idx, key=f"{key_prefix}_select", help=help_text or None)
        if choice == "<custom>":
            return st.text_input(f"{label} (custom)", value=current or "", key=f"{key_prefix}_custom", help=help_text or None)
        return choice
    return st.text_input(label, value=current or "", key=f"{key_prefix}_custom", help=help_text or None)

def _multiselect_with_custom(label: str, options: list[str], current: list[str], key_prefix: str, help_text: str = "") -> list[str]:
    current = current or []
    in_options = [c for c in current if c in options]
    extra = [c for c in current if c not in options]
    selected = st.multiselect(label, options, default=in_options, key=f"{key_prefix}_multi", help=help_text or None)
    extra_text = st.text_input(f"{label} (custom, comma separated)", value=", ".join(extra), key=f"{key_prefix}_custom", help=help_text or None)
    extra_list = _split_csv_list(extra_text)
    return selected + extra_list

def _save_editor_content_to(out_path: pathlib.Path) -> bool:
    try:
        raw = yaml.safe_load(st.session_state.get("editor_content", ""))
        load_job_from_raw(raw)
    except Exception as e:
        st.error(f"Config ist ungueltig: {e}")
        return False
    out_path.write_text(st.session_state.get("editor_content", ""), encoding="utf-8")
    st.success(f"Gespeichert: {out_path.name}")
    return True

st.subheader("Job-Config bearbeiten")
st.markdown(f"[Hilfe zur Job-Config Anleitung]({README_PATH.as_uri()})")
editor_job_name = st.selectbox(
    "Config-Datei für Editor",
    [p.name for p in job_files],
    index=[p.name for p in job_files].index(job_name),
    help="Welche Datei im Editor geladen wird.",
)
editor_path = CONFIG_DIR / editor_job_name

if editor_job_name != job_name:
    st.warning(f"Achtung: Run verwendet **{job_name}**, Editor zeigt **{editor_job_name}**.")

if "editor_content" not in st.session_state:
    st.session_state["editor_content"] = editor_path.read_text(encoding="utf-8")
    st.session_state["editor_loaded"] = editor_job_name

if st.session_state.get("editor_loaded") != editor_job_name:
    st.info(f"Editor zeigt: {st.session_state.get('editor_loaded')}. Klicke \"Neu laden\" um {editor_job_name} zu laden.")

valid_cfg, cfg_error = _validate_editor_content() if "editor_content" in st.session_state else (False, "Kein Editor-Inhalt")
st.subheader("Config Status")
if valid_cfg:
    st.success("Config ist gueltig.")
else:
    st.error(f"Config ist ungueltig: {cfg_error}")

job_ui = _load_job_from_editor() or job
source_columns = _get_first_columns(source_dir, job_ui.source.file_types) if job_ui and dir_ok(source_dir) else []
target_columns = _get_first_columns(target_dir, job_ui.target.file_types) if job_ui and dir_ok(target_dir) else []

st.divider()
st.subheader("Presets/Templates")
st.caption("Vorlagen speichern und laden (ohne die aktiven Config-Dateien zu ueberschreiben).")

template_files = sorted([p for p in TEMPLATE_DIR.glob("*.yml")] + [p for p in TEMPLATE_DIR.glob("*.yaml")] + [p for p in TEMPLATE_DIR.glob("*.json")])
template_names = [p.name for p in template_files]

colTpl1, colTpl2 = st.columns(2)
with colTpl1:
    tpl_choice = st.selectbox(
        "Template laden",
        template_names,
        key="tpl_choice",
        help="Laedt eine Vorlage in den Editor.",
    ) if template_names else None
    if st.button("Template in Editor laden"):
        if not tpl_choice:
            st.warning("Keine Templates gefunden.")
        else:
            tpl_path = TEMPLATE_DIR / tpl_choice
            st.session_state["editor_content"] = tpl_path.read_text(encoding="utf-8")
            st.session_state["editor_loaded"] = tpl_choice
            st.success(f"Template geladen: {tpl_choice}")
with colTpl2:
    tpl_save_name = st.text_input(
        "Template speichern als",
        value="",
        key="tpl_save_name",
        help="Speichert den aktuellen Editor-Inhalt als Template.",
    )
    if st.button("Als Template speichern"):
        name = tpl_save_name.strip()
        if not name:
            st.error("Bitte Template-Namen angeben.")
        else:
            if not name.endswith((".yml", ".yaml", ".json")):
                name = name + ".yml"
            _save_editor_content_to(TEMPLATE_DIR / name)

st.divider()
st.subheader("Configs verwalten")
colCfg1, colCfg2 = st.columns(2)
with colCfg1:
    load_name = st.selectbox(
        "Config laden",
        [p.name for p in job_files],
        index=[p.name for p in job_files].index(editor_job_name),
        key="cfg_load_name",
        help="Laedt eine Config in den Editor.",
    )
    if st.button("In Editor laden"):
        load_path = CONFIG_DIR / load_name
        st.session_state["editor_content"] = load_path.read_text(encoding="utf-8")
        st.session_state["editor_loaded"] = load_name
        st.success(f"Geladen: {load_name}")
with colCfg2:
    save_as_name = st.text_input(
        "Speichern unter",
        value=editor_job_name,
        key="cfg_save_as_name",
        help="Speichert den aktuellen Editor-Inhalt unter neuem Namen.",
    )
    if st.button("Speichern unter..."):
        name = save_as_name.strip()
        if not name:
            st.error("Bitte Dateinamen angeben.")
        else:
            if not name.endswith((".yml", ".yaml", ".json")):
                name = name + ".yml"
            _save_editor_content_to(CONFIG_DIR / name)

st.divider()
st.subheader("Kunden-Match (UI)")
st.caption("Input/Output fuer den Kundenname-Match auswaehlen. Falls Input = Zelle, bitte Zell-ID angeben (z. B. E5).")

editor_raw = _load_editor_raw()
if editor_raw is None and "editor_content" in st.session_state:
    st.warning("Editor-Config ist ungueltig. UI-Aenderungen koennen erst nach einem gueltigen Config-Inhalt uebernommen werden.")

cm_raw = (editor_raw or {}).get("customer_match") or {}
cm_source = cm_raw.get("source") or {}
cm_target = cm_raw.get("target") or {}

source_options = {
    "excel_cell_or_csv_row": "Excel-Zelle oder CSV-Zeile (Standard)",
    "excel_cell": "Excel-Zelle",
    "csv_row": "CSV-Zeile",
    "column": "Spalte aus der Source-Zeile",
    "filename": "Dateiname (ohne Regex)",
}
target_options = {
    "column": "Ziel-Spalte",
    "filename": "Zieldateiname (ohne Regex)",
}

default_source_type = str(cm_source.get("type") or "excel_cell_or_csv_row")
default_target_type = str(cm_target.get("type") or "column")
regex_in_use = default_source_type == "filename_regex" or default_target_type == "filename_regex"
show_advanced = st.checkbox(
    "Erweitert (Regex anzeigen)",
    value=regex_in_use,
    key="cm_advanced",
    help="Blendet Regex-Optionen fuer Source/Target ein.",
)
if show_advanced:
    source_options["filename_regex"] = "Dateiname per Regex"
    target_options["filename_regex"] = "Zieldateiname per Regex"

default_excel_cell = str(
    cm_source.get("excel_cell")
    or ((editor_raw or {}).get("source", {}).get("customer", {}) or {}).get("excel_cell")
    or "E5"
)
default_source_column = str(cm_source.get("column") or "")
default_source_regex = str(cm_source.get("filename_regex") or "")
default_target_column = str(
    cm_target.get("column")
    or ((editor_raw or {}).get("target", {}).get("match", {}) or {}).get("column")
    or ""
)
default_target_regex = str(cm_target.get("filename_regex") or "")

colCM1, colCM2 = st.columns(2)
with colCM1:
    source_type = st.selectbox(
        "Input (Quelle)",
        list(source_options.keys()),
        index=list(source_options.keys()).index(default_source_type) if default_source_type in source_options else 0,
        format_func=lambda k: source_options[k],
        key="cm_source_type",
        help="Wie der Kundenname aus der Source ermittelt wird.",
    )
with colCM2:
    target_type = st.selectbox(
        "Output (Ziel)",
        list(target_options.keys()),
        index=list(target_options.keys()).index(default_target_type) if default_target_type in target_options else 0,
        format_func=lambda k: target_options[k],
        key="cm_target_type",
        help="Wie der Kundenname im Target gematcht wird.",
    )

source_excel_cell = ""
source_column = ""
source_regex = ""
target_column = ""
target_regex = ""

if source_type in ("excel_cell_or_csv_row", "excel_cell"):
    source_excel_cell = st.text_input(
        "Excel-Zelle (z. B. E5)",
        value=default_excel_cell,
        key="cm_excel_cell",
        help="Zell-ID fuer den Kundenname bei Excel-Dateien.",
    )
if source_type == "column":
    source_column = _select_with_custom(
        "Source-Spalte",
        source_columns,
        default_source_column,
        "cm_source_column",
        help_text="Spalte mit dem Kundenname in der Source-Zeile.",
    )
if source_type == "filename_regex":
    source_regex = st.text_input(
        "Source-Dateiname Regex",
        value=default_source_regex,
        key="cm_source_regex",
        help="Regex muss den Kundenname aus dem Dateinamen liefern.",
    )

if target_type == "column":
    target_column = _select_with_custom(
        "Ziel-Spalte",
        target_columns,
        default_target_column,
        "cm_target_column",
        help_text="Spalte im Target fuer Kundenname-Match.",
    )
if target_type == "filename_regex":
    target_regex = st.text_input(
        "Ziel-Dateiname Regex",
        value=default_target_regex,
        key="cm_target_regex",
        help="Regex muss den Kundenname aus dem Target-Dateinamen liefern.",
    )

colApplyCM1, colApplyCM2 = st.columns(2)
with colApplyCM1:
    apply_cm = st.button("In Editor uebernehmen", key="cm_apply")
with colApplyCM2:
    apply_cm_save = st.button("Uebernehmen + speichern", key="cm_apply_save")

if apply_cm or apply_cm_save:
    if editor_raw is None:
        st.error("Editor-Config ist ungueltig. Bitte erst gueltigen Inhalt herstellen.")
    else:
        cm_new = {k: v for k, v in cm_raw.items() if k not in ("source", "target")}
        source_cfg = {"type": source_type}
        target_cfg = {"type": target_type}

        if source_type in ("excel_cell_or_csv_row", "excel_cell") and source_excel_cell.strip():
            source_cfg["excel_cell"] = source_excel_cell.strip()
            editor_raw.setdefault("source", {}).setdefault("customer", {})["excel_cell"] = source_excel_cell.strip()
        if source_type == "column" and source_column.strip():
            source_cfg["column"] = source_column.strip()
        if source_type == "filename_regex" and source_regex.strip():
            source_cfg["filename_regex"] = source_regex.strip()

        if target_type == "column" and target_column.strip():
            target_cfg["column"] = target_column.strip()
        if target_type == "filename_regex" and target_regex.strip():
            target_cfg["filename_regex"] = target_regex.strip()

        cm_new["source"] = source_cfg
        cm_new["target"] = target_cfg
        editor_raw["customer_match"] = cm_new

        st.session_state["editor_content"] = _dump_config(editor_raw, editor_job_name)
        if apply_cm_save:
            _save_editor_content_to(editor_path)
        st.success("Kunden-Match aktualisiert.")

st.divider()
st.subheader("Matching-Regeln (UI)")
st.caption("Fuzzy/Exact/Contains und Normalisierung fuer Element- und Kunden-Matching.")

editor_raw = _load_editor_raw()
if editor_raw is None and "editor_content" in st.session_state:
    st.warning("Editor-Config ist ungueltig. UI-Aenderungen koennen erst nach einem gueltigen Config-Inhalt uebernommen werden.")

match_norm_options = ["strip", "lower", "collapse_spaces"]
src_elem = (editor_raw or {}).get("source", {}).get("element", {}) or {}
cust_match = (editor_raw or {}).get("customer_match", {}) or {}
tgt_match = (editor_raw or {}).get("target", {}).get("match", {}) or {}
has_customer_match = "customer_match" in (editor_raw or {})

elem_fuzzy_default = float(src_elem.get("fuzzy_threshold", 0.78))
cust_mode_default = str(cust_match.get("mode") or tgt_match.get("mode") or "exact")
cust_norm_default = list(cust_match.get("normalize") or tgt_match.get("normalize") or [])
cust_fuzzy_default = float(cust_match.get("fuzzy_threshold", 0.78))
tgt_mode_default = str(tgt_match.get("mode") or "exact")
tgt_norm_default = list(tgt_match.get("normalize") or [])
target_match_column_default = str(tgt_match.get("column") or "")

colMR1, colMR2, colMR3 = st.columns(3)
with colMR1:
    elem_fuzzy = st.number_input(
        "Element-Fuzzy-Schwelle",
        min_value=0.0,
        max_value=1.0,
        step=0.01,
        value=elem_fuzzy_default,
        key="mr_elem_fuzzy",
        help="Threshold fuer Element->Target Fuzzy-Matching.",
    )
with colMR2:
    cust_mode = st.selectbox(
        "Customer-Match Modus",
        ["exact", "contains", "fuzzy"],
        index=["exact", "contains", "fuzzy"].index(cust_mode_default) if cust_mode_default in ["exact", "contains", "fuzzy"] else 0,
        key="mr_cust_mode",
        help="Match-Logik fuer Kundenname.",
    )
    cust_norm = st.multiselect(
        "Customer-Normalize",
        match_norm_options,
        default=[x for x in cust_norm_default if x in match_norm_options],
        key="mr_cust_norm",
        help="Normalisierungsschritte vor dem Match.",
    )
with colMR3:
    tgt_mode = st.selectbox(
        "Target-Match Modus",
        ["exact", "contains", "fuzzy"],
        index=["exact", "contains", "fuzzy"].index(tgt_mode_default) if tgt_mode_default in ["exact", "contains", "fuzzy"] else 0,
        key="mr_tgt_mode",
        help="Match-Logik fuer Ziel-Label.",
    )
    tgt_norm = st.multiselect(
        "Target-Normalize",
        match_norm_options,
        default=[x for x in tgt_norm_default if x in match_norm_options],
        key="mr_tgt_norm",
        help="Normalisierungsschritte fuer Ziel-Label.",
    )
    target_match_column = _select_with_custom(
        "Target-Match Spalte",
        target_columns,
        target_match_column_default,
        "mr_target_match_column",
        help_text="Spalte im Target, die fuer das Matching verwendet wird.",
    )

cust_fuzzy = None
if cust_mode == "fuzzy":
    cust_fuzzy = st.number_input(
        "Customer-Fuzzy-Schwelle",
        min_value=0.0,
        max_value=1.0,
        step=0.01,
        value=cust_fuzzy_default,
        key="mr_cust_fuzzy",
        help="Threshold fuer Fuzzy-Matching des Kundennamens.",
    )

colMRApply1, colMRApply2 = st.columns(2)
with colMRApply1:
    apply_mr = st.button("Matching-Regeln uebernehmen", key="mr_apply")
with colMRApply2:
    apply_mr_save = st.button("Uebernehmen + speichern", key="mr_apply_save")

if apply_mr or apply_mr_save:
    if editor_raw is None:
        st.error("Editor-Config ist ungueltig. Bitte erst gueltigen Inhalt herstellen.")
    else:
        editor_raw.setdefault("source", {}).setdefault("element", {})["fuzzy_threshold"] = float(elem_fuzzy)

        if has_customer_match:
            cust_match_new = dict(cust_match)
        else:
            fallback_excel_cell = str(((editor_raw or {}).get("source", {}).get("customer", {}) or {}).get("excel_cell") or "E5")
            fallback_target_col = str(((editor_raw or {}).get("target", {}).get("match", {}) or {}).get("column") or "Label DE")
            cust_match_new = {
                "source": {"type": "excel_cell_or_csv_row", "excel_cell": fallback_excel_cell},
                "target": {"type": "column", "column": fallback_target_col},
            }
        cust_match_new["mode"] = cust_mode
        cust_match_new["normalize"] = cust_norm
        if cust_fuzzy is not None:
            cust_match_new["fuzzy_threshold"] = float(cust_fuzzy)
        editor_raw["customer_match"] = cust_match_new

        tgt_match_new = dict(tgt_match)
        tgt_match_new["mode"] = tgt_mode
        tgt_match_new["normalize"] = tgt_norm
        if target_match_column.strip():
            tgt_match_new["column"] = target_match_column.strip()
        editor_raw.setdefault("target", {})["match"] = tgt_match_new

        st.session_state["editor_content"] = _dump_config(editor_raw, editor_job_name)
        if apply_mr_save:
            _save_editor_content_to(editor_path)
        st.success("Matching-Regeln aktualisiert.")

st.divider()
st.subheader("Source-Element Mapping (UI)")
st.caption("Element-Spalte, Mapping-Tabelle und Regeln fuer das Ziel-File (contains/regex).")

editor_raw = _load_editor_raw()
if editor_raw is None and "editor_content" in st.session_state:
    st.warning("Editor-Config ist ungueltig. UI-Aenderungen koennen erst nach einem gueltigen Config-Inhalt uebernommen werden.")

src_elem = (editor_raw or {}).get("source", {}).get("element", {}) or {}
elem_column_default = str(src_elem.get("column") or "Element")

elem_map = src_elem.get("map", {}) or {}
map_rows = [{"element": k, "target_filename": v} for k, v in elem_map.items()]
map_df = st.data_editor(
    pd.DataFrame(map_rows, columns=["element", "target_filename"]),
    num_rows="dynamic",
    key="elem_map_editor",
    use_container_width=True,
    help="Direktes Mapping Element-Text -> Ziel-Dateiname.",
)

rules_rows = []
for r in (src_elem.get("rules", []) or []):
    when = r.get("when", {}) or {}
    target = r.get("target", {}) or {}
    rules_rows.append({
        "name": r.get("name", ""),
        "when_contains": ", ".join([str(x) for x in (when.get("contains") or [])]),
        "when_regex": str(when.get("regex") or ""),
        "filename_contains": ", ".join([str(x) for x in (target.get("filename_contains") or [])]),
        "label_contains": ", ".join([str(x) for x in (target.get("label_contains") or [])]),
        "filename_regex": str(target.get("filename_regex") or ""),
        "prefer_benefit_number": bool(target.get("prefer_benefit_number") or False),
    })

rules_df = st.data_editor(
    pd.DataFrame(
        rules_rows,
        columns=[
            "name",
            "when_contains",
            "when_regex",
            "filename_contains",
            "label_contains",
            "filename_regex",
            "prefer_benefit_number",
        ],
    ),
    num_rows="dynamic",
    key="elem_rules_editor",
    use_container_width=True,
    help="Regeln: when_contains/regex und target (filename/label contains).",
)

elem_column = _select_with_custom(
    "Element-Spalte",
    source_columns,
    elem_column_default,
    "elem_column_input",
    help_text="Spalte mit Elementnamen in der Source.",
)

colElemApply1, colElemApply2 = st.columns(2)
with colElemApply1:
    apply_elem = st.button("Source-Element Mapping uebernehmen", key="elem_apply")
with colElemApply2:
    apply_elem_save = st.button("Uebernehmen + speichern", key="elem_apply_save")

if apply_elem or apply_elem_save:
    if editor_raw is None:
        st.error("Editor-Config ist ungueltig. Bitte erst gueltigen Inhalt herstellen.")
    else:
        new_map = {}
        for _, row in map_df.iterrows():
            k = str(row.get("element") or "").strip()
            v = str(row.get("target_filename") or "").strip()
            if k and v:
                new_map[k] = v

        new_rules = []
        for _, row in rules_df.iterrows():
            name = str(row.get("name") or "").strip()
            when_contains = _split_csv_list(row.get("when_contains"))
            when_regex = str(row.get("when_regex") or "").strip()
            filename_contains = _split_csv_list(row.get("filename_contains"))
            label_contains = _split_csv_list(row.get("label_contains"))
            filename_regex = str(row.get("filename_regex") or "").strip()
            prefer_benefit_number = bool(row.get("prefer_benefit_number") or False)

            has_any = any([
                name,
                when_contains,
                when_regex,
                filename_contains,
                label_contains,
                filename_regex,
                prefer_benefit_number,
            ])
            if not has_any:
                continue

            when_cfg = {}
            if when_contains:
                when_cfg["contains"] = when_contains
            if when_regex:
                when_cfg["regex"] = when_regex

            target_cfg = {}
            if filename_contains:
                target_cfg["filename_contains"] = filename_contains
            if label_contains:
                target_cfg["label_contains"] = label_contains
            if filename_regex:
                target_cfg["filename_regex"] = filename_regex
            if prefer_benefit_number:
                target_cfg["prefer_benefit_number"] = True

            rule_cfg = {}
            if name:
                rule_cfg["name"] = name
            if when_cfg:
                rule_cfg["when"] = when_cfg
            if target_cfg:
                rule_cfg["target"] = target_cfg
            new_rules.append(rule_cfg)

        editor_raw.setdefault("source", {}).setdefault("element", {})["column"] = elem_column.strip() or "Element"
        editor_raw["source"]["element"]["map"] = new_map
        editor_raw["source"]["element"]["rules"] = new_rules

        st.session_state["editor_content"] = _dump_config(editor_raw, editor_job_name)
        if apply_elem_save:
            _save_editor_content_to(editor_path)
        st.success("Source-Element Mapping aktualisiert.")

st.divider()
st.subheader("Content Extraction (UI)")
st.caption("Spalten pro Sprache und Zielspalten definieren. Spalten koennen Name, col_letter: oder col_index: sein.")

editor_raw = _load_editor_raw()
if editor_raw is None and "editor_content" in st.session_state:
    st.warning("Editor-Config ist ungueltig. UI-Aenderungen koennen erst nach einem gueltigen Config-Inhalt uebernommen werden.")

content_cfg = (editor_raw or {}).get("source", {}).get("content", {}) or {}
content_mode = str(content_cfg.get("mode") or "row_columns")
content_mode = st.selectbox(
    "Content-Modus",
    ["row_columns", "join_column"],
    index=["row_columns", "join_column"].index(content_mode) if content_mode in ["row_columns", "join_column"] else 0,
    key="content_mode",
    help="row_columns: pro Zeile; join_column: alles zusammenfassen.",
)

langs_cfg = (content_cfg.get("languages") or {}) if isinstance(content_cfg, dict) else {}
lang_keys = list(langs_cfg.keys())
extra_langs = st.session_state.get("content_extra_langs", [])
for lang in extra_langs:
    if lang not in lang_keys:
        lang_keys.append(lang)

colLangAdd1, colLangAdd2 = st.columns([2, 1])
with colLangAdd1:
    new_lang = st.text_input(
        "Neue Sprache (z. B. IT)",
        value="",
        key="content_new_lang",
        help="Fuegt eine neue Sprachsektion hinzu.",
    )
with colLangAdd2:
    if st.button("Sprache hinzufuegen"):
        code = new_lang.strip()
        if code and code not in lang_keys:
            st.session_state["content_extra_langs"] = extra_langs + [code]
            st.success(f"Sprache hinzugefuegt: {code}")

lang_ui_values = {}
for lang in lang_keys:
    cfg = langs_cfg.get(lang, {}) or {}
    cols = cfg.get("columns") or cfg.get("column") or []
    if not isinstance(cols, list):
        cols = [cols]
    target_col = str(cfg.get("target_column") or "")
    with st.expander(f"Sprache: {lang}", expanded=True):
        selected_cols = _multiselect_with_custom(
            "Source-Spalten",
            source_columns,
            [str(x) for x in cols],
            f"content_{lang}_cols",
            help_text="Spalten, aus denen der Text fuer diese Sprache gelesen wird.",
        )
        selected_target = _select_with_custom(
            "Ziel-Spalte",
            target_columns,
            target_col,
            f"content_{lang}_target",
            help_text="Spalte im Target, in die der Text geschrieben wird.",
        )
        lang_ui_values[lang] = {"columns": selected_cols, "target_column": selected_target}

colContentApply1, colContentApply2 = st.columns(2)
with colContentApply1:
    apply_content = st.button("Content Extraction uebernehmen", key="content_apply")
with colContentApply2:
    apply_content_save = st.button("Uebernehmen + speichern", key="content_apply_save")

if apply_content or apply_content_save:
    if editor_raw is None:
        st.error("Editor-Config ist ungueltig. Bitte erst gueltigen Inhalt herstellen.")
    else:
        new_langs = {}
        for lang, cfg in lang_ui_values.items():
            cols = [c for c in cfg.get("columns", []) if str(c).strip()]
            target_col = str(cfg.get("target_column") or "").strip()
            if not lang or not cols or not target_col:
                continue
            new_langs[lang] = {"columns": cols, "target_column": target_col}

        editor_raw.setdefault("source", {}).setdefault("content", {})["mode"] = content_mode
        editor_raw["source"]["content"]["languages"] = new_langs

        st.session_state["editor_content"] = _dump_config(editor_raw, editor_job_name)
        if apply_content_save:
            _save_editor_content_to(editor_path)
        st.success("Content Extraction aktualisiert.")

st.divider()
st.subheader("Customer CSV-Row Match (UI)")
st.caption("CSV-Zeile fuer Kundenname: row_match Spalte/Equals und Value-Spalten-Praeferenz.")

editor_raw = _load_editor_raw()
if editor_raw is None and "editor_content" in st.session_state:
    st.warning("Editor-Config ist ungueltig. UI-Aenderungen koennen erst nach einem gueltigen Config-Inhalt uebernommen werden.")

cust_cfg = (editor_raw or {}).get("source", {}).get("customer", {}) or {}
cust_csv = (cust_cfg.get("csv") or {}) if isinstance(cust_cfg, dict) else {}
row_match = cust_csv.get("row_match", {}) or {}

row_match_col_default = str(row_match.get("column") or "Element")
row_match_equals_default = str(row_match.get("equals") or "Firmenname")
value_pref_default = ", ".join([str(x) for x in (cust_csv.get("value_column_preference") or [])])

colCSV1, colCSV2, colCSV3 = st.columns(3)
with colCSV1:
    row_match_col = _select_with_custom("Row-Match Spalte", source_columns, row_match_col_default, "csv_row_match_col")
with colCSV2:
    row_match_equals = st.text_input(
        "Row-Match Equals",
        value=row_match_equals_default,
        key="csv_row_match_equals",
        help="Wert der Row-Match Spalte, z. B. Firmenname.",
    )
with colCSV3:
    value_pref = st.text_input(
        "Value-Column Preference (CSV)",
        value=value_pref_default,
        key="csv_value_pref",
        help="Kommagetrennte Liste der bevorzugten Wertspalten (Name oder col_index:).",
    )

colCSVApply1, colCSVApply2 = st.columns(2)
with colCSVApply1:
    apply_csv = st.button("CSV-Row Match uebernehmen", key="csv_apply")
with colCSVApply2:
    apply_csv_save = st.button("Uebernehmen + speichern", key="csv_apply_save")

if apply_csv or apply_csv_save:
    if editor_raw is None:
        st.error("Editor-Config ist ungueltig. Bitte erst gueltigen Inhalt herstellen.")
    else:
        cust = editor_raw.setdefault("source", {}).setdefault("customer", {})
        cust.setdefault("method", "excel_cell_or_csv_row")
        cust.setdefault("excel_cell", str(cust_cfg.get("excel_cell") or "E5"))
        csv_cfg = cust.setdefault("csv", {})
        csv_cfg["row_match"] = {
            "column": row_match_col.strip() or "Element",
            "equals": row_match_equals.strip() or "Firmenname",
        }
        csv_cfg["value_column_preference"] = _split_csv_list(value_pref)

        st.session_state["editor_content"] = _dump_config(editor_raw, editor_job_name)
        if apply_csv_save:
            _save_editor_content_to(editor_path)
        st.success("CSV-Row Match aktualisiert.")

st.divider()
st.subheader("Target Behavior (UI)")
st.caption("Schreibverhalten fuer Ziel-Dateien.")

editor_raw = _load_editor_raw()
if editor_raw is None and "editor_content" in st.session_state:
    st.warning("Editor-Config ist ungueltig. UI-Aenderungen koennen erst nach einem gueltigen Config-Inhalt uebernommen werden.")

behavior = (editor_raw or {}).get("target", {}).get("behavior", {}) or {}
overwrite_existing = bool(behavior.get("overwrite_existing", True))
write_only_if_present = bool(behavior.get("write_only_if_present", True))
strict_single_match = bool(behavior.get("strict_single_match", False))

colTB1, colTB2, colTB3 = st.columns(3)
with colTB1:
    overwrite_existing_ui = st.checkbox(
        "overwrite_existing",
        value=overwrite_existing,
        key="tb_overwrite_existing",
        help="Ueberschreibt bestehende Inhalte im Target.",
    )
with colTB2:
    write_only_if_present_ui = st.checkbox(
        "write_only_if_present",
        value=write_only_if_present,
        key="tb_write_only_if_present",
        help="Schreibt nur wenn fuer die Sprache Text vorhanden ist.",
    )
with colTB3:
    strict_single_match_ui = st.checkbox(
        "strict_single_match",
        value=strict_single_match,
        key="tb_strict_single_match",
        help="Blockiert bei mehreren Treffern.",
    )

colTBApply1, colTBApply2 = st.columns(2)
with colTBApply1:
    apply_tb = st.button("Target Behavior uebernehmen", key="tb_apply")
with colTBApply2:
    apply_tb_save = st.button("Uebernehmen + speichern", key="tb_apply_save")

if apply_tb or apply_tb_save:
    if editor_raw is None:
        st.error("Editor-Config ist ungueltig. Bitte erst gueltigen Inhalt herstellen.")
    else:
        editor_raw.setdefault("target", {}).setdefault("behavior", {})
        editor_raw["target"]["behavior"]["overwrite_existing"] = bool(overwrite_existing_ui)
        editor_raw["target"]["behavior"]["write_only_if_present"] = bool(write_only_if_present_ui)
        editor_raw["target"]["behavior"]["strict_single_match"] = bool(strict_single_match_ui)

        st.session_state["editor_content"] = _dump_config(editor_raw, editor_job_name)
        if apply_tb_save:
            _save_editor_content_to(editor_path)
        st.success("Target Behavior aktualisiert.")

st.divider()
st.subheader("Output Optionen (UI)")
st.caption("Report-Ausgabe und Collision-Log steuern.")

editor_raw = _load_editor_raw()
if editor_raw is None and "editor_content" in st.session_state:
    st.warning("Editor-Config ist ungueltig. UI-Aenderungen koennen erst nach einem gueltigen Config-Inhalt uebernommen werden.")

out_cfg = (editor_raw or {}).get("output", {}) or {}
write_reports_ui = bool(out_cfg.get("write_reports", True))
reports_exclude_text_ui = bool(out_cfg.get("reports_exclude_text", True))
write_collisions_ui = bool(out_cfg.get("write_collisions", True))

colOut1, colOut2, colOut3 = st.columns(3)
with colOut1:
    write_reports_ui = st.checkbox(
        "write_reports",
        value=write_reports_ui,
        key="out_write_reports",
        help="Erzeugt report.csv, blocked.csv und audit.json.",
    )
with colOut2:
    reports_exclude_text_ui = st.checkbox(
        "reports_exclude_text",
        value=reports_exclude_text_ui,
        key="out_reports_exclude_text",
        help="Entfernt Textinhalte aus Reports.",
    )
with colOut3:
    write_collisions_ui = st.checkbox(
        "write_collisions",
        value=write_collisions_ui,
        key="out_write_collisions",
        help="Schreibt collisions.csv bei widerspruechlichen Writes.",
    )

colOutApply1, colOutApply2 = st.columns(2)
with colOutApply1:
    apply_out = st.button("Output Optionen uebernehmen", key="out_apply")
with colOutApply2:
    apply_out_save = st.button("Uebernehmen + speichern", key="out_apply_save")

if apply_out or apply_out_save:
    if editor_raw is None:
        st.error("Editor-Config ist ungueltig. Bitte erst gueltigen Inhalt herstellen.")
    else:
        editor_raw.setdefault("output", {})
        editor_raw["output"]["write_reports"] = bool(write_reports_ui)
        editor_raw["output"]["reports_exclude_text"] = bool(reports_exclude_text_ui)
        editor_raw["output"]["write_collisions"] = bool(write_collisions_ui)

        st.session_state["editor_content"] = _dump_config(editor_raw, editor_job_name)
        if apply_out_save:
            _save_editor_content_to(editor_path)
        st.success("Output Optionen aktualisiert.")

st.divider()
st.subheader("Social Links (UI)")
st.caption("Plattform-Overrides fuer Keywords/Domains und Vorschau auf social_unmapped.csv.")

editor_raw = _load_editor_raw()
if editor_raw is None and "editor_content" in st.session_state:
    st.warning("Editor-Config ist ungueltig. UI-Aenderungen koennen erst nach einem gueltigen Config-Inhalt uebernommen werden.")

social_cfg = (editor_raw or {}).get("social", {}) or {}
platforms = (social_cfg.get("platforms") or {}) if isinstance(social_cfg, dict) else {}
platform_rows = []
for name, cfg in (platforms or {}).items():
    if cfg is None:
        continue
    platform_rows.append({
        "platform": str(name),
        "keywords": ", ".join([str(x) for x in (cfg.get("keywords") or [])]),
        "domains": ", ".join([str(x) for x in (cfg.get("domains") or [])]),
    })

platform_df = st.data_editor(
    pd.DataFrame(platform_rows, columns=["platform", "keywords", "domains"]),
    num_rows="dynamic",
    key="social_platform_editor",
    use_container_width=True,
    help="Pro Plattform Keywords und Domains (Komma-getrennt).",
)

colSocialApply1, colSocialApply2 = st.columns(2)
with colSocialApply1:
    apply_social = st.button("Social Overrides uebernehmen", key="social_apply")
with colSocialApply2:
    apply_social_save = st.button("Uebernehmen + speichern", key="social_apply_save")

if apply_social or apply_social_save:
    if editor_raw is None:
        st.error("Editor-Config ist ungueltig. Bitte erst gueltigen Inhalt herstellen.")
    else:
        new_platforms = {}
        for _, row in platform_df.iterrows():
            name = str(row.get("platform") or "").strip()
            keywords = _split_csv_list(row.get("keywords"))
            domains = _split_csv_list(row.get("domains"))
            if not name:
                continue
            new_platforms[name] = {"keywords": keywords, "domains": domains}
        if new_platforms:
            editor_raw["social"] = {"platforms": new_platforms}
        else:
            editor_raw.pop("social", None)
        st.session_state["editor_content"] = _dump_config(editor_raw, editor_job_name)
        if apply_social_save:
            _save_editor_content_to(editor_path)
        st.success("Social Overrides aktualisiert.")

colSocial1, colSocial2 = st.columns(2)
with colSocial1:
    if st.button("social_unmapped.csv laden (letzter Run)"):
        latest_social = _find_latest_social_unmapped(output_dir)
        if latest_social is None:
            st.warning("Keine social_unmapped.csv im Output gefunden.")
        else:
            df_social = pd.read_csv(latest_social, dtype=object)
            st.dataframe(df_social, use_container_width=True)
            st.caption(f"Quelle: {latest_social}")
with colSocial2:
    base = pathlib.Path(output_dir)
    run_dirs = [p for p in base.glob("run_*") if p.is_dir()] if base.exists() else []
    run_names = [p.name for p in sorted(run_dirs, reverse=True)]
    run_choice = st.selectbox(
        "Run-Ordner",
        run_names,
        key="social_run_choice",
        help="Aus welchem Run social_unmapped.csv geladen wird.",
    ) if run_names else None
    if st.button("social_unmapped.csv laden (ausgewaehlter Run)"):
        if not run_choice:
            st.warning("Keine Run-Ordner gefunden.")
        else:
            candidate = base / run_choice / "social_unmapped.csv"
            if not candidate.exists():
                st.warning("social_unmapped.csv nicht gefunden.")
            else:
                df_social = pd.read_csv(candidate, dtype=object)
                st.dataframe(df_social, use_container_width=True)
                st.caption(f"Quelle: {candidate}")

st.divider()
st.subheader("Validation Helpers")
st.caption("Spalten pruefen und aufgeloeste Spalten anzeigen.")

job_ui = _load_job_from_editor() or job

if st.button("Check columns in source/target"):
    if job_ui is None:
        st.error("Job-Config ist ungueltig. Bitte im Editor korrigieren.")
    elif not dir_ok(source_dir) or not dir_ok(target_dir):
        st.error("Bitte gueltige Ordner angeben.")
    else:
        try:
            src_paths = list_files(source_dir, job_ui.source.file_types)
            tgt_paths = list_files(target_dir, job_ui.target.file_types)
            if not src_paths or not tgt_paths:
                st.warning("Keine Source/Target-Dateien gefunden.")
            else:
                src_path = src_paths[0]
                tgt_path = tgt_paths[0]
                df_src = read_table(src_path)
                df_tgt = read_table(tgt_path)

                missing_src = []
                elem_col = job_ui.source.element.column
                if _resolve_column(df_src.columns, elem_col) is None:
                    missing_src.append(elem_col)
                for lang_cfg in job_ui.source.content.languages.values():
                    for col_spec in lang_cfg.columns:
                        if _resolve_column(df_src.columns, col_spec) is None:
                            missing_src.append(col_spec)
                if job_ui.customer_match.source.type == "column":
                    col = job_ui.customer_match.source.column or ""
                    if col and _resolve_column(df_src.columns, col) is None:
                        missing_src.append(col)
                if job_ui.source.customer and job_ui.source.customer.csv:
                    csv_col = job_ui.source.customer.csv.row_match_column
                    if csv_col and _resolve_column(df_src.columns, csv_col) is None:
                        missing_src.append(csv_col)

                missing_tgt = []
                match_col = job_ui.target.match.column
                if _resolve_column(df_tgt.columns, match_col) is None:
                    missing_tgt.append(match_col)
                for lang_cfg in job_ui.source.content.languages.values():
                    if _resolve_column(df_tgt.columns, lang_cfg.target_column) is None:
                        missing_tgt.append(lang_cfg.target_column)
                if job_ui.customer_match.target.type == "column":
                    col = job_ui.customer_match.target.column or ""
                    if col and _resolve_column(df_tgt.columns, col) is None:
                        missing_tgt.append(col)

                st.write("Source-Datei:", src_path.name)
                st.dataframe(pd.DataFrame({"missing_source_columns": sorted(set(missing_src))}))
                st.write("Target-Datei:", tgt_path.name)
                st.dataframe(pd.DataFrame({"missing_target_columns": sorted(set(missing_tgt))}))
        except Exception as e:
            st.error(str(e))

if st.button("Show resolved column names"):
    if job_ui is None:
        st.error("Job-Config ist ungueltig. Bitte im Editor korrigieren.")
    elif not dir_ok(source_dir) or not dir_ok(target_dir):
        st.error("Bitte gueltige Ordner angeben.")
    else:
        try:
            src_paths = list_files(source_dir, job_ui.source.file_types)
            tgt_paths = list_files(target_dir, job_ui.target.file_types)
            if not src_paths or not tgt_paths:
                st.warning("Keine Source/Target-Dateien gefunden.")
            else:
                src_path = src_paths[0]
                tgt_path = tgt_paths[0]
                df_src = read_table(src_path)
                df_tgt = read_table(tgt_path)

                src_specs = [job_ui.source.element.column]
                for lang_cfg in job_ui.source.content.languages.values():
                    src_specs.extend(lang_cfg.columns)
                if job_ui.customer_match.source.type == "column" and job_ui.customer_match.source.column:
                    src_specs.append(job_ui.customer_match.source.column)
                if job_ui.source.customer and job_ui.source.customer.csv:
                    src_specs.append(job_ui.source.customer.csv.row_match_column)

                tgt_specs = [job_ui.target.match.column]
                for lang_cfg in job_ui.source.content.languages.values():
                    tgt_specs.append(lang_cfg.target_column)
                if job_ui.customer_match.target.type == "column" and job_ui.customer_match.target.column:
                    tgt_specs.append(job_ui.customer_match.target.column)

                src_rows = [{"spec": s, "resolved": _resolve_column(df_src.columns, s)} for s in src_specs]
                tgt_rows = [{"spec": s, "resolved": _resolve_column(df_tgt.columns, s)} for s in tgt_specs]

                st.write("Source:", src_path.name)
                st.dataframe(pd.DataFrame(src_rows), use_container_width=True)
                st.write("Target:", tgt_path.name)
                st.dataframe(pd.DataFrame(tgt_rows), use_container_width=True)
        except Exception as e:
            st.error(str(e))

st.divider()
st.subheader("Preview Tools")
st.caption("Beispielzeile -> Ziel-Datei vor dem Run.")

if dir_ok(source_dir) and dir_ok(target_dir) and job_ui is not None:
    src_paths = list_files(source_dir, job_ui.source.file_types)
    tgt_paths = list_files(target_dir, job_ui.target.file_types)
    src_names = [p.name for p in src_paths]
    if src_names:
        src_choice = st.selectbox(
            "Source-Datei",
            src_names,
            key="preview_source_file",
            help="Datei fuer die Vorschau auswaehlen.",
        )
        src_path = next((p for p in src_paths if p.name == src_choice), None)
        if src_path is not None:
            try:
                items = extract_row_items(src_path, job_ui)
                if items:
                    idx = st.number_input(
                        "Row-Index",
                        min_value=0,
                        max_value=max(len(items) - 1, 0),
                        step=1,
                        value=0,
                        key="preview_row_index",
                        help="Welche Zeile aus der Source fuer die Vorschau genutzt wird.",
                    )
                    if st.button("Preview anzeigen"):
                        elem, texts, row = items[int(idx)]
                        cust = extract_customer_key(src_path, job_ui, row=row)
                        label_map = runner_mod._build_target_label_map(tgt_paths)
                        name_map = {p.name: p for p in tgt_paths}
                        element_map = job_ui.source.element.map
                        resolved = runner_mod._resolve_target_path(
                            elem,
                            texts.get("DE"),
                            texts.get("FR"),
                            label_map,
                            name_map,
                            element_map,
                            job_ui.source.element.fuzzy_threshold,
                            job_ui.source.element.rules,
                            job_ui.social,
                        )
                        st.write("Element:", elem)
                        st.write("Customer:", cust)
                        st.write("Text DE:", texts.get("DE"))
                        st.write("Text FR:", texts.get("FR"))
                        if resolved is None:
                            st.warning("Kein Ziel gefunden.")
                        else:
                            st.write("Ziel-Label:", resolved[0])
                            st.write("Ziel-Datei:", resolved[1].name)
                else:
                    st.warning("Keine verwertbaren Zeilen gefunden.")
            except Exception as e:
                st.error(str(e))
    else:
        st.warning("Keine Source-Dateien gefunden.")

colE1, colE2, colE3 = st.columns(3)
with colE1:
    if st.button("Neu laden"):
        st.session_state["editor_content"] = editor_path.read_text(encoding="utf-8")
        st.session_state["editor_loaded"] = editor_job_name
with colE2:
    if st.button("Validieren"):
        try:
            raw = yaml.safe_load(st.session_state["editor_content"])
            load_job_from_raw(raw)
            st.success("Config ist gültig.")
        except Exception as e:
            st.error(str(e))
with colE3:
    save_name = st.text_input(
        "Dateiname",
        value=editor_job_name,
        help="Dateiname fuer das Speichern des Editor-Inhalts.",
    )
    if st.button("Speichern"):
        name = save_name.strip()
        if not name:
            st.error("Bitte Dateinamen angeben.")
        else:
            if not name.endswith((".yml", ".yaml", ".json")):
                name = name + ".yml"
            out_path = CONFIG_DIR / name
            out_path.write_text(st.session_state["editor_content"], encoding="utf-8")
            st.success(f"Gespeichert: {out_path.name}")

st.text_area(
    "Job-Config (YAML/JSON)",
    key="editor_content",
    height=320,
    help="Direktes Bearbeiten der Config.",
)

st.divider()
st.subheader("Run")
st.subheader("Dry Run / Vorschau")
if st.button("Dry Run ausführen"):
    if job is None:
        st.error("Job-Config ist ungültig. Bitte im Editor korrigieren.")
    elif not dir_ok(source_dir) or not dir_ok(target_dir):
        st.error("Bitte gültige Ordner angeben.")
    else:
        try:
            df = dry_run(job, source_dir, target_dir, strict_single_match_override=strict_override)
            st.dataframe(df, use_container_width=True)
            st.session_state["last_dry_run"] = df
        except Exception as e:
            st.error(str(e))

st.subheader("Run / Schreiben")
if st.button("Run starten (schreibt nur in Output)"):
    if job is None:
        st.error("Job-Config ist ungültig. Bitte im Editor korrigieren.")
    elif not dir_ok(source_dir) or not dir_ok(target_dir):
        st.error("Bitte gültige Ordner angeben.")
    else:
        try:
            result = run(
                job,
                source_dir=source_dir,
                target_dir=target_dir,
                output_base_dir=output_dir,
                strict_single_match_override=strict_override,
                write_reports_override=write_reports,
                write_collisions_override=write_collisions,
            )
            st.success(f"Status: {result['status']}")
            st.code(result.get("output_dir", ""))
            if result["status"] == "OK":
                st.code(result.get("updated_targets_dir", ""))
        except Exception as e:
            st.error(str(e))
