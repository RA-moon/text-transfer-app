# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

app_name = "RA-moon's List-Wizard"
project_root = Path(__file__).resolve().parent.parent
packaging_dir = Path(__file__).resolve().parent
icon_path = packaging_dir / "AppIcon.icns"

block_cipher = None

a = Analysis(
    ['run_app.py'],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name=app_name,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=[str(icon_path)],
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name=app_name,
)
app = BUNDLE(
    coll,
    name=f"{app_name}.app",
    icon=str(icon_path),
    bundle_identifier=None,
)
