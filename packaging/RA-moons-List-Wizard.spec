# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
import sys

app_name = "RA-moon's List-Wizard"
spec_path = Path(__file__).resolve() if "__file__" in globals() else Path(sys.argv[0]).resolve()
project_root = spec_path.parent.parent
packaging_dir = spec_path.parent
icon_path = packaging_dir / "AppIcon.icns"
icon_arg = str(icon_path) if icon_path.exists() else None

block_cipher = None

a = Analysis(
    [str(project_root / "run_app.py")],
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
    icon=[icon_arg] if icon_arg else [],
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
    icon=str(icon_path) if icon_path.exists() else None,
    bundle_identifier=None,
)
