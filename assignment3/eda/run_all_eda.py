#!/usr/bin/env python3
"""
Run all EDA analyses sequentially.
"""

import subprocess
import sys
from pathlib import Path


def run_eda_script(script_name):
    """Run a single EDA script."""
    script_path = Path(__file__).parent / script_name
    script_dir = script_path.parent
    if script_path.exists():
        print(f"\n{'='*50}")
        print(f"Running {script_name}")
        print('='*50)
        try:
            result = subprocess.run([sys.executable, str(script_path)], capture_output=True, text=True, cwd=script_dir)
            print(result.stdout)
            if result.stderr:
                print("STDERR:", result.stderr)
            if result.returncode != 0:
                print(f"Error running {script_name}: return code {result.returncode}")
        except Exception as e:
            print(f"Failed to run {script_name}: {e}")
    else:
        print(f"Script {script_name} not found")

def main():
    """Run all EDA scripts."""
    eda_scripts = [
        'eda_movies_metadata.py',
        'eda_credits.py',
        'eda_keywords.py',
        'eda_links.py',
        'eda_links_small.py',
        'eda_ratings.py',
        'eda_ratings_small.py'
    ]

    print("Running all EDA analyses...")

    for script in eda_scripts:
        run_eda_script(script)

    print(f"\n{'='*50}")
    print("All EDA analyses completed!")
    print('='*50)

if __name__ == '__main__':
    main()