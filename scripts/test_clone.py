"""
Clean-Clone Test for CUMTA TransitPulse.
Simulates a clean git clone by copying ONLY tracked repository files
(obtained from git ls-files) into an isolated temporary directory,
then verifying that all 5 tabs render successfully in sample data mode.
"""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def run_clean_clone_test():
    repo_root = Path(__file__).resolve().parent.parent

    # Retrieve tracked files using git ls-files
    res = subprocess.run(
        ["git", "ls-files"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    tracked_files = [line.strip() for line in res.stdout.splitlines() if line.strip()]

    temp_clone = Path(tempfile.gettempdir()) / "transitpulse_clean_clone_test"
    if temp_clone.exists():
        shutil.rmtree(temp_clone, ignore_errors=True)
    temp_clone.mkdir(parents=True, exist_ok=True)

    print(f"Creating isolated clean clone at: {temp_clone}")
    print(f"Copying {len(tracked_files)} tracked files...")

    for rel_path in tracked_files:
        src = repo_root / rel_path
        dst = temp_clone / rel_path
        if src.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)

    # Create isolated virtual environment
    venv_dir = temp_clone / ".venv"
    print(f"Creating isolated virtual environment at {venv_dir}...")
    subprocess.run(
        [sys.executable, "-m", "venv", "--system-site-packages", str(venv_dir)],
        check=True,
    )
    venv_python = (
        str(venv_dir / "Scripts" / "python.exe")
        if os.name == "nt"
        else str(venv_dir / "bin" / "python")
    )

    # Test all 5 tabs in sample mode inside temp_clone
    tabs = [
        (1, "Dashboard"),
        (2, "Chennai Map"),
        (3, "Diagnose"),
        (4, "Actions & Simulator"),
        (5, "Data & Replay"),
    ]

    all_passed = True
    print("\nTesting tab rendering in clean clone (sample data mode):")
    for tab_num, tab_name in tabs:
        test_code = f"""
import sys, os
os.chdir(r'{temp_clone}')
sys.path.insert(0, r'{temp_clone}')
os.environ['TRANSITPULSE_DATA_DIR'] = 'sample_data'
import streamlit as st
st.query_params['tab'] = '{tab_num}'
import app
print('TAB_{tab_num}_RENDER_SUCCESS')
"""
        cmd = [venv_python, "-c", test_code]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0 and f"TAB_{tab_num}_RENDER_SUCCESS" in proc.stdout:
            print(f"  [PASS] Tab {tab_num} ({tab_name}): Rendered successfully")
        else:
            print(f"  [FAIL] Tab {tab_num} ({tab_name}) failed:\n{proc.stderr[-400:]}")
            all_passed = False

    # Cleanup temp clone
    shutil.rmtree(temp_clone, ignore_errors=True)

    if all_passed:
        print("\nALL 5 TABS RENDERED SUCCESSFULLY IN CLEAN-CLONE SAMPLE MODE!")
    else:
        print("\nCLEAN-CLONE TEST HAD FAILURES!")
    return all_passed


if __name__ == "__main__":
    success = run_clean_clone_test()
    sys.exit(0 if success else 1)
