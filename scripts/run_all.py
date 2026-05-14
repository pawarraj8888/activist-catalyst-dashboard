"""
run_all.py  -  Master pipeline runner
Run this locally: python scripts/run_all.py
GitHub Actions runs this on schedule automatically
"""

import subprocess
import sys
import os

scripts = [
    "scripts/pull_wrds.py",
    "scripts/pull_edgar.py",
    "scripts/pull_options.py",
    "scripts/pull_short_interest.py",
]

print("=" * 50)
print("ACTIVIST CATALYST DASHBOARD - Data Pipeline")
print("=" * 50)

for script in scripts:
    print(f"\nRunning {script}...")
    result = subprocess.run([sys.executable, script], capture_output=False)
    if result.returncode != 0:
        print(f"WARNING: {script} exited with code {result.returncode}")

print("\nAll scripts complete. Data saved to /data/")
print("Open index.html in browser to view dashboard.")