# utils/path.py

# BELOW: From Databricks Labs - Blueprint
from pathlib import Path

def find_project_root(start_path: str) -> Path:
    """
    Returns Path object for the nearest folder with pyproject.toml or setup.py.
    Typical usage: find_project_root(__file__)
    """
    path = Path(start_path).resolve()
    for parent in [path] + list(path.parents):
        if (parent / "pyproject.toml").exists() or (parent / "setup.py").exists():
            return parent
    raise RuntimeError("Project root not found (no pyproject.toml or setup.py)")