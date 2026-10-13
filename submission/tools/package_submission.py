"""Build the canonical delivery from the current explicitly selected revision."""
import subprocess,sys,shutil
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
subprocess.run([sys.executable,str(ROOT/'submission/tools/package_revision.py'),'--run',str(ROOT/'C_delivery/results/C_leaf_study_20261014'),'--report-dir',str(ROOT/'submission')],check=True)
shutil.copyfile(ROOT/'submission/C_leaf_study_20261014_Submission.zip',ROOT/'submission/DSA5208_Project2_Submission.zip')
