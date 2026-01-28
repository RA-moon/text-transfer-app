from __future__ import annotations
import subprocess
import sys

def main():
    cmd = [
        sys.executable, "-m", "streamlit", "run", "app.py",
        "--server.address", "127.0.0.1",
        "--server.port", "8501",
        "--browser.gatherUsageStats", "false",
    ]
    raise SystemExit(subprocess.call(cmd))

if __name__ == "__main__":
    main()
