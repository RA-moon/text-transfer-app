from __future__ import annotations
import pathlib
import shutil
import yaml
import streamlit as st

from engine.job_loader import load_job, load_job_from_raw, JobConfigError
from engine.runner import dry_run, run

APP_NAME = "RA-moon's List-Wizard"
APP_TITLE = "RA-moon's List-Wizard (Local Only)"
README_PATH = pathlib.Path("README.md").resolve()
BASE_DIR = pathlib.Path.home() / "Desktop" / APP_NAME
CONFIG_DIR = BASE_DIR / "configs"
DEFAULT_SOURCE_DIR = BASE_DIR / "input" / "datensammlungen"
DEFAULT_TARGET_DIR = BASE_DIR / "input" / "zieldateien"
DEFAULT_OUTPUT_DIR = BASE_DIR / "output"

def ensure_app_dirs():
    (BASE_DIR / "input" / "datensammlungen").mkdir(parents=True, exist_ok=True)
    (BASE_DIR / "input" / "zieldateien").mkdir(parents=True, exist_ok=True)
    (BASE_DIR / "output").mkdir(parents=True, exist_ok=True)
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

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

st.caption(f"Arbeitsordner: {BASE_DIR}")
st.caption(f"Configs: {CONFIG_DIR}")

job_files = list_job_files(CONFIG_DIR)
if not job_files:
    st.error("Keine Job-Dateien im Config-Ordner gefunden.")
    st.stop()

job_name = st.selectbox("Job auswählen", [p.name for p in job_files])
job_path = CONFIG_DIR / job_name

source_dir = st.text_input("Source-Ordner (Datensammlungen)", value=str(DEFAULT_SOURCE_DIR))
target_dir = st.text_input("Target-Ordner (Zieldateien)", value=str(DEFAULT_TARGET_DIR))
output_dir = st.text_input("Output-Basisordner", value=str(DEFAULT_OUTPUT_DIR))

colA, colB, colC = st.columns(3)
with colA:
    strict_override = st.checkbox("Strict single match (blockiert bei >1 Match)", value=False)
with colB:
    write_reports = st.checkbox("Lokale Reports schreiben (ohne Textinhalte)", value=True)
with colC:
    write_collisions = st.checkbox("Collision-Log schreiben", value=True)

def dir_ok(p: str) -> bool:
    pp = pathlib.Path(p)
    return pp.exists() and pp.is_dir()

job = None
try:
    job = load_job(job_path)
except JobConfigError as e:
    st.error(f"Job-Config ungültig: {e}")

st.subheader("Job-Config bearbeiten")
st.markdown(f"[Hilfe zur Job-Config Anleitung]({README_PATH.as_uri()})")
editor_job_name = st.selectbox("Config-Datei für Editor", [p.name for p in job_files], index=[p.name for p in job_files].index(job_name))
editor_path = CONFIG_DIR / editor_job_name

if editor_job_name != job_name:
    st.warning(f"Achtung: Run verwendet **{job_name}**, Editor zeigt **{editor_job_name}**.")

if "editor_content" not in st.session_state:
    st.session_state["editor_content"] = editor_path.read_text(encoding="utf-8")
    st.session_state["editor_loaded"] = editor_job_name

if st.session_state.get("editor_loaded") != editor_job_name:
    st.info(f"Editor zeigt: {st.session_state.get('editor_loaded')}. Klicke \"Neu laden\" um {editor_job_name} zu laden.")

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
    save_name = st.text_input("Dateiname", value=editor_job_name)
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

st.text_area("Job-Config (YAML/JSON)", key="editor_content", height=320)

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
