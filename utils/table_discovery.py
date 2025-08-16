# src/datalake_pipeline_expectations/package/utils/table_discovery.py
from __future__ import annotations

from typing import Iterable, List, Optional, Tuple, Dict, Union, Literal
from pathlib import Path
import re
import sys

try:
    from databricks.sdk import WorkspaceClient
except Exception:
    WorkspaceClient = None  # optional dep

try:
    from pyspark.sql import SparkSession
except Exception:
    SparkSession = None  # type: ignore

Mode = Literal["catalog", "schema", "pipeline", "table"]


def _parse_schema_fqn(schema_fqn: str) -> Tuple[str, str]:
    parts = schema_fqn.split(".")
    if len(parts) != 2:
        raise RuntimeError(f"schema must be 'catalog.schema', got: {schema_fqn}")
    return parts[0], parts[1]


def _is_view(table_type: Optional[str]) -> bool:
    t = (table_type or "").upper()
    return t in {"VIEW", "MATERIALIZED_VIEW"}


def _normalize_txt_name(name: Union[str, bool, None]) -> str:
    if name is True or not name:
        return "tree.txt"
    p = Path(str(name).strip())
    return p.name if p.suffix.lower() == ".txt" else f"{p.stem}.txt"


def _write_text(path: Union[str, Path], text: str, overwrite: bool = True) -> Path:
    s = str(path)
    if s.startswith("dbfs:"):
        try:
            dbutils.fs.put(s, text, overwrite=overwrite)  # type: ignore[name-defined]
            return Path(s)
        except NameError:
            raise RuntimeError("dbutils not available; use a local/Volumes path instead.")
    p = Path(s)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite: {p}")
    p.write_text(text, encoding="utf-8")
    return p


class TableDiscovery:
    """
    SDK-first UC discovery utility with optional column materialization and tree rendering.

    Constructor parameters:
      - sdk_client: optional WorkspaceClient (inject for tests)
      - include_views: list views as tables (default False)
      - exclude_prefixes: list of table-name prefixes to drop (exact startswith, case-insensitive)
      - excludeTableRegex: simple string treated as a case-insensitive **prefix** to exclude (no regex syntax)
      - spark: optional SparkSession for DESCRIBE fallback when SDK can't return column types

    Discovery methods (stable returns):
      - discover_pipeline_tables(pipeline_name, *, list_columns=False, assume_schema=None)
      - discover_schema_tables('cat.schema', *, list_columns=False)
      - discover_catalog_tables('cat', *, list_columns=False)

      Returns:
        • if list_columns == False: List[str] of FQNs (pipeline may include bare names unless assume_schema provided)
        • if list_columns == True : Dict[str, Dict[col->type]] mapping FQN (or name) → {col: dtype}

    Tree rendering:
      - render_tree(mode, target, *, list_columns=False, construct_tree=False,
                    save_tree: bool|str=False, output_dir: Optional[str|Path]=None,
                    assume_schema: Optional[str]=None, spacious=True)
        -> Tuple[str, Optional[Path]]  # (rendered_text, saved_path)
    """

    def __init__(
        self,
        *,
        sdk_client: Optional[object] = None,
        include_views: bool = False,
        exclude_prefixes: Optional[Iterable[str]] = None,
        excludeTableRegex: Optional[str] = None,
        spark: Optional["SparkSession"] = None,
    ) -> None:
        if WorkspaceClient is None and sdk_client is None:
            raise RuntimeError("Databricks SDK not installed; install `databricks-sdk` to use discovery.")

        self.w = sdk_client or WorkspaceClient()
        self.include_views = include_views

        # Exclusions
        self.exclude_prefixes = [p.lower() for p in (exclude_prefixes or [])]
        # excludeTableRegex is intentionally simple: just a case-insensitive **prefix** match
        self.exclude_prefix_single = (excludeTableRegex or "").lower().strip()

        self.spark = spark  # only used for DESCRIBE fallback

        # Handle SDK surface diffs across versions
        self._tables = getattr(self.w, "tables", None) or getattr(getattr(self.w, "unity_catalog", None), "tables", None)
        self._schemas = getattr(self.w, "schemas", None) or getattr(getattr(self.w, "unity_catalog", None), "schemas", None)
        self._catalogs = getattr(self.w, "catalogs", None) or getattr(getattr(self.w, "unity_catalog", None), "catalogs", None)
        if not self._tables or not self._schemas:
            raise RuntimeError("SDK client doesn’t expose Unity Catalog tables/schemas on this workspace client.")

    # ---------------------------
    # Public: discovery
    # ---------------------------

    def discover_pipeline_tables(
        self,
        pipeline_name: str,
        *,
        list_columns: bool = False,
        assume_schema: Optional[str] = None,  # qualify bare names as cat.schema.name
        page_size: int = 250,
        empty_page_tolerance: int = 2,
    ):
        pipelines = list(self.w.pipelines.list_pipelines())
        pl = next((p for p in pipelines if p.name == pipeline_name), None)
        if not pl:
            raise RuntimeError(f"Pipeline '{pipeline_name}' not found via SDK.")
        if not getattr(pl, "latest_updates", None):
            raise RuntimeError(f"Pipeline '{pipeline_name}' has no updates recorded.")
        latest_update = pl.latest_updates[0].update_id

        events = self.w.pipelines.list_pipeline_events(pipeline_id=pl.pipeline_id, max_results=page_size)

        names, empty = set(), 0
        it = iter(events)
        while True:
            page = []
            try:
                for _ in range(page_size): page.append(next(it))
            except StopIteration:
                pass
            page_names = {
                getattr(ev.origin, "flow_name", None)
                for ev in page
                if getattr(ev.origin, "update_id", None) == latest_update and getattr(ev.origin, "flow_name", None)
            }
            page_names.discard(None)
            names |= page_names
            empty = 0 if page_names else empty + 1
            if empty >= empty_page_tolerance or not page:
                break

        if not names:
            raise RuntimeError(f"No output tables found for pipeline '{pipeline_name}' via event logs.")

        # qualify names if needed
        fqdns: List[str] = []
        for n in sorted(names):
            if n.count(".") == 2:
                fqdns.append(n)
            elif assume_schema:
                cat, sch = _parse_schema_fqn(assume_schema)
                fqdns.append(f"{cat}.{sch}.{n}")
            else:
                fqdns.append(n)  # cannot fetch columns without FQN

        if not list_columns:
            # Apply excludes to the final list (where we can)
            return [t for t in fqdns if self._keep_table_name(t.split(".")[-1])]

        # columns requested
        resolvable = [t for t in fqdns if t.count(".") == 2 and self._keep_table_name(t.split(".")[-1])]
        cols = self._materialize_columns(resolvable)
        # keep non-resolvable names (or excluded) as {} for a transparent signal
        out: Dict[str, Dict[str, str]] = {t: cols.get(t, {}) for t in fqdns if self._keep_table_name(t.split(".")[-1])}
        return out

    def discover_schema_tables(self, schema_fqn: str, *, list_columns: bool = False):
        catalog, schema = _parse_schema_fqn(schema_fqn)
        self._assert_schema_exists(catalog, schema)
        tables = self._list_tables_for_schema(catalog, schema)
        if not tables:
            raise RuntimeError(f"No tables found under schema {catalog}.{schema} (include_views={self.include_views}).")
        return self._materialize_columns(tables) if list_columns else tables

    def discover_catalog_tables(self, catalog: str, *, list_columns: bool = False):
        self._assert_catalog_exists(catalog)
        schemas = self._list_schema_names(catalog)
        if not schemas:
            raise RuntimeError(f"Catalog '{catalog}' has no schemas visible.")
        all_tables: List[str] = []
        for sch in schemas:
            try:
                all_tables.extend(self._list_tables_for_schema(catalog, sch))
            except Exception as e:
                print(f"[WARN] skipping {catalog}.{sch}: {e}", file=sys.stderr)
        all_tables = sorted(set(all_tables))
        if not all_tables:
            raise RuntimeError(f"No tables found under catalog {catalog} (include_views={self.include_views}).")
        return self._materialize_columns(all_tables) if list_columns else all_tables

    # ---------------------------
    # Public: tree rendering
    # ---------------------------

    def render_tree(
        self,
        mode: Mode,
        target: str,
        *,
        list_columns: bool = False,
        construct_tree: bool = False,
        save_tree: Union[bool, str] = False,
        output_dir: Optional[Union[str, Path]] = None,
        assume_schema: Optional[str] = None,
        spacious: bool = True,
    ) -> Tuple[str, Optional[Path]]:
        """
        Build + render an ASCII tree for UC objects. Prints if construct_tree=True.
        If save_tree is True -> saves as 'tree.txt'; if str -> saves as that name (normalized to .txt).
        Returns (rendered_text, saved_path or None).
        """
        if save_tree and not construct_tree:
            raise ValueError("save_tree requires construct_tree=True")

        root_label, nested = self._build_nested(mode, target, list_columns=list_columns, assume_schema=assume_schema)
        text = self._render_ascii(root_label, nested, spacious=spacious)

        if construct_tree:
            print(text)
            if save_tree:
                fname = _normalize_txt_name(save_tree)
                out_dir = Path(output_dir) if output_dir else Path.cwd()
                saved = _write_text(out_dir / fname, text, overwrite=True)
                return text, saved
        return text, None

    # ---------------------------
    # Internals: structure + render
    # ---------------------------

    def _build_nested(
        self, mode: Mode, target: str, *, list_columns: bool, assume_schema: Optional[str]
    ) -> Tuple[str, Dict]:
        if mode == "catalog":
            if list_columns:
                cols = self.discover_catalog_tables(target, list_columns=True)  # Dict[fqn->{col:dtype}]
                out: Dict[str, Dict] = {}
                for fqn, cmap in cols.items():
                    _, schema, table = fqn.split(".")
                    out.setdefault(schema, {})[table] = dict(sorted(cmap.items()))
                out = {s: dict(sorted(out[s].items())) for s in sorted(out)}
                return target, out
            tables = self.discover_catalog_tables(target, list_columns=False)
            out: Dict[str, Dict] = {}
            for fqn in tables:
                _, schema, table = fqn.split(".")
                out.setdefault(schema, {})[table] = {}
            out = {s: dict(sorted(out[s].items())) for s in sorted(out)}
            return target, out

        if mode == "schema":
            if list_columns:
                cols = self.discover_schema_tables(target, list_columns=True)
                _, schema = target.split(".", 1)
                out = {schema: {}}
                for fqn, cmap in cols.items():
                    table = fqn.split(".")[2]
                    out[schema][table] = dict(sorted(cmap.items()))
                out[schema] = dict(sorted(out[schema].items()))
                return target, out
            tables = self.discover_schema_tables(target, list_columns=False)
            _, schema = target.split(".", 1)
            out = {schema: {}}
            for fqn in tables:
                table = fqn.split(".")[2]
                out[schema][table] = {}
            out[schema] = dict(sorted(out[schema].items()))
            return target, out

        if mode == "pipeline":
            if list_columns:
                cols = self.discover_pipeline_tables(target, list_columns=True, assume_schema=assume_schema)
                out: Dict[str, Dict] = {}
                for name, cmap in cols.items():
                    if name.count(".") == 2:
                        _, schema, table = name.split(".")
                        out.setdefault(schema, {})[table] = dict(sorted(cmap.items()))
                    else:
                        out.setdefault("<unqualified>", {})[name] = dict(sorted(cmap.items()))
                out = {s: dict(sorted(out[s].items())) for s in sorted(out)}
                return target, out
            names = self.discover_pipeline_tables(target, list_columns=False, assume_schema=assume_schema)
            out: Dict[str, Dict] = {}
            for n in names:
                if n.count(".") == 2:
                    _, schema, table = n.split(".")
                    out.setdefault(schema, {})[table] = {}
                else:
                    out.setdefault("<unqualified>", {})[n] = {}
            out = {s: dict(sorted(out[s].items())) for s in sorted(out)}
            return target, out

        if mode == "table":
            if target.count(".") != 2:
                raise ValueError("table mode requires fully-qualified 'catalog.schema.table'")
            cat, schema, table = target.split(".")
            if list_columns:
                cols = self.discover_schema_tables(f"{cat}.{schema}", list_columns=True)
                cmap = cols.get(target, {})
                return target, {schema: {table: dict(sorted(cmap.items()))}}
            return target, {schema: {table: {}}}

        raise ValueError("mode must be one of: catalog|schema|pipeline|table")

    def _render_ascii(self, root_label: str, nested: Dict, *, spacious: bool) -> str:
        lines: List[str] = [f"{root_label}/"]

        def walk(node: Dict, prefix: str = ""):
            items = sorted(node.items(), key=lambda kv: kv[0].lower())
            for i, (name, child) in enumerate(items):
                is_last = i == len(items) - 1
                branch = "└── " if is_last else "├── "
                is_leaf_cols = isinstance(child, dict) and all(isinstance(v, str) for v in child.values())
                label = f"{name}/" if not is_leaf_cols else name
                lines.append(prefix + branch + label)
                next_prefix = prefix + ("    " if is_last else "│   ")
                if isinstance(child, dict):
                    if is_leaf_cols:
                        cols = sorted(child.items(), key=lambda kv: kv[0].lower())
                        for j, (cname, dtype) in enumerate(cols):
                            c_last = j == len(cols) - 1
                            c_branch = "└── " if c_last else "├── "
                            lines.append(next_prefix + c_branch + f"{cname} : {dtype}")
                        if spacious and not is_last:
                            lines.append(prefix + "│")
                    else:
                        walk(child, next_prefix)
                        if spacious and not is_last:
                            lines.append(prefix + "│")

        walk(nested)
        return "\n".join(lines) + "\n"

    # ---------------------------
    # Internals: listing + columns
    # ---------------------------

    def _assert_catalog_exists(self, catalog: str) -> None:
        try:
            if self._catalogs is not None:
                if not any(c.name == catalog for c in self._catalogs.list()):  # type: ignore[attr-defined]
                    raise RuntimeError(f"Catalog not found: {catalog}")
            else:
                if not any(True for _ in self._schemas.list(catalog_name=catalog)):  # type: ignore[attr-defined]
                    raise RuntimeError(f"Catalog not found: {catalog}")
        except Exception as e:
            raise RuntimeError(f"Failed to verify catalog '{catalog}': {e}")

    def _assert_schema_exists(self, catalog: str, schema: str) -> None:
        try:
            if not any(s.name == schema for s in self._schemas.list(catalog_name=catalog)):  # type: ignore[attr-defined]
                raise RuntimeError(f"Schema not found: {catalog}.{schema}")
        except Exception as e:
            raise RuntimeError(f"Failed to verify schema '{catalog}.{schema}': {e}")

    def _list_schema_names(self, catalog: str) -> List[str]:
        try:
            return [s.name for s in self._schemas.list(catalog_name=catalog)]  # type: ignore[attr-defined]
        except Exception as e:
            raise RuntimeError(f"Failed to list schemas for catalog '{catalog}': {e}")

    def _keep_table_name(self, tbl_name: str) -> bool:
        n = tbl_name.lower()
        if self.exclude_prefixes and any(n.startswith(p) for p in self.exclude_prefixes):
            return False
        if self.exclude_prefix_single and n.startswith(self.exclude_prefix_single):
            return False
        return True

    def _list_tables_for_schema(self, catalog: str, schema: str) -> List[str]:
        out: List[str] = []
        for t in self._tables.list(catalog_name=catalog, schema_name=schema):  # type: ignore[attr-defined]
            tname = getattr(t, "name", None)
            ttype = getattr(t, "table_type", None)
            if not tname:
                continue
            if (not self.include_views) and _is_view(ttype):
                continue
            if not self._keep_table_name(tname):
                continue
            out.append(f"{t.catalog_name}.{t.schema_name}.{tname}")
        return out

    # ---- columns ----
    def _tables_get(self, fqn: str):
        get = getattr(self._tables, "get", None)
        if get is None:
            raise RuntimeError("SDK client missing tables.get")
        try:
            return get(name=fqn)  # type: ignore[misc]
        except TypeError:
            return get(full_name=fqn)  # type: ignore[misc]

    def _columns_via_sdk(self, fqn: str) -> Optional[Dict[str, str]]:
        try:
            info = self._tables_get(fqn)
            cols = getattr(info, "columns", None)
            if not cols:
                return None
            out: Dict[str, str] = {}
            for c in cols:
                name = getattr(c, "name", None)
                ttxt = getattr(c, "type_text", None) or getattr(c, "type_name", None)
                if name and ttxt:
                    out[name] = str(ttxt)
            return out or None
        except Exception:
            return None

    def _columns_via_spark_describe(self, fqn: str) -> Optional[Dict[str, str]]:
        if not self.spark:
            return None
        try:
            rows = self.spark.sql(f"DESCRIBE {fqn}").collect()
            cols: Dict[str, str] = {}
            for r in rows:
                col = (getattr(r, "col_name", None) or "").strip()
                dt  = (getattr(r, "data_type", None) or "").strip()
                if not col or col.startswith("#"):
                    if col == "# Partition Information":
                        break
                    continue
                cols[col] = dt
            return cols or None
        except Exception:
            return None

    def _materialize_columns(self, tables: Iterable[str]) -> Dict[str, Dict[str, str]]:
        result: Dict[str, Dict[str, str]] = {}
        for fqn in tables:
            if fqn.count(".") != 2:
                result[fqn] = {}
                continue
            cols = self._columns_via_sdk(fqn) or self._columns_via_spark_describe(fqn) or {}
            result[fqn] = cols
        return result