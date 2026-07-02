import sys
from pathlib import Path

# Make the repo root importable so `scanner` resolves regardless of cwd.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
