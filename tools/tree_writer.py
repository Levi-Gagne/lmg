# tools/tree_writer.py

from __future__ import annotations
from pathlib import Path
from typing import Optional, List

class TreeWriter:
    """
    Minimal ASCII tree generator with optional 'spacious' styling.

    - Required: root (highest directory to start from)
    - Optional:
        * file_name  : output file name (default: 'TREE.txt')
        * output_dir : where to write the file (default: root)
        * spacious   : if True, insert vertical spacer lines between sibling sections
                       at each directory level (default: True)

    Output is written as an ASCII tree with directory slashes and guides.
    """

    def __init__(
        self,
        root: str | Path,
        file_name: str = "TREE.txt",
        output_dir: Optional[str | Path] = None,
        spacious: bool = True,
    ):
        self.root = Path(root).resolve()
        self.file_name = file_name or "TREE.txt"
        self.output_dir = Path(output_dir).resolve() if output_dir else self.root
        self.spacious = spacious

    def _children(self, d: Path) -> List[Path]:
        # Directories first, then files; case-insensitive sort
        return sorted(d.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))

    def _render(self) -> str:
        lines: List[str] = [self.root.name.rstrip("/") + "/"]

        def rec(cur: Path, prefix: str = "") -> None:
            kids = self._children(cur)
            for i, p in enumerate(kids):
                is_last = (i == len(kids) - 1)
                branch = "└── " if is_last else "├── "
                name = p.name + ("/" if p.is_dir() else "")
                lines.append(prefix + branch + name)

                if p.is_dir():
                    # Dive into the directory
                    next_prefix = prefix + ("    " if is_last else "│   ")
                    rec(p, next_prefix)

                    # Optional spacer line between sibling sections (directories only)
                    if self.spacious and not is_last:
                        # Place a vertical bar aligned with the current prefix column
                        # Example:
                        # root
                        # ├── A/
                        # │   ...A contents...
                        # │
                        # └── B/
                        lines.append(prefix + "│")

        rec(self.root)
        return "\n".join(lines) + "\n"

    def write(self) -> Path:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        out_path = self.output_dir / self.file_name
        out_path.write_text(self._render(), encoding="utf-8")
        print(f"Wrote {out_path}")
        return out_path

    # One-liner entry point
    @classmethod
    def write_tree(
        cls,
        root: str | Path,
        file_name: str = "TREE.txt",
        output_dir: Optional[str | Path] = None,
        spacious: bool = True,
    ) -> Path:
        return cls(root, file_name=file_name, output_dir=output_dir, spacious=spacious).write()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Write an ASCII TREE.txt for a directory.")
    ap.add_argument("root", help="Root directory to start from (highest node)")
    ap.add_argument("--file-name", default="TREE.txt", help="Output file name (default: TREE.txt)")
    ap.add_argument("--output-dir", default=None, help="Directory to write output (default: root)")
    ap.add_argument("--compact", action="store_true", help="Compact style (no spacer lines)")
    args = ap.parse_args()
    TreeWriter.write_tree(
        args.root,
        file_name=args.file_name,
        output_dir=args.output_dir,
        spacious=not args.compact,
    )

# --- Notebook usage examples (commented) ---
# 1) If 'tools' is already importable (package installed or sys.path configured):
# from tools.tree_writer import TreeWriter
# TreeWriter.write_tree(root="/absolute/or/relative/path/to/repo")  # writes TREE.txt to root
#
# 2) If running a notebook from src/ and tools/ is one level up:
# from pathlib import Path
# import sys
# sys.path.insert(0, str(Path("..", "tools").resolve()))  # <-- only to import tree_writer
# from tree_writer import TreeWriter
# TreeWriter.write_tree(root="/any/path/you/want", spacious=True)   # 'spacious' is the new style (default)
# # or compact (original tight layout):
# TreeWriter.write_tree(root="..", file_name="STRUCTURE.txt", output_dir="../docs", spacious=False)
