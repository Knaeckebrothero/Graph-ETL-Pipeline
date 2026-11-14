"""The folder path contains a hyphen (experiments/thesis-b), so the package is imported by path."""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))   # experiments/thesis-b  ->  import validator
REPO = HERE.parents[3]                          # repository root
