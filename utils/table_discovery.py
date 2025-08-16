# src/datalake_pipeline_expectations/package/utils/table_discovery.py
from __future__ import annotations

from typing import Iterable, List, Optional, Tuple
import re

try:
    from databricks.sdk import WorkspaceClient
except ImportError as e:
    WorkspaceClient = None
    print(f"[ERROR] databricks.sdk import failed: {e}")


def _parse_schema_fqn(schema_fqn: str) -> Tuple[str, str]:
    parts = schema_fqn.split(".")
    if len(parts) != 2:
        raise RuntimeError(f"schema must be 'catalog.schema', got: {schema_fqn}")
    return parts[0], parts[1]


def _is_view(table_type: Optional[str]) -> bool:
    t = (table_type or "").upper()
    return t in {"VIEW", "MATERIALIZED_VIEW"}


class TableDiscovery:
    """
    SDK-first discovery utility:
      - discover_pipeline_tables(pipeline_name)
      - discover_schema_tables('cat.schema', include_views=?)
      - discover_catalog_tables('cat', include_views=?)

    Shared config lives on the instance (sdk_client, include_views, exclude filters).
    No caching. Prints lightweight traces for notebook visibility.
    """

    def __init__(
        self,
        *,
        sdk_client: Optional[object] = None,
        include_views: bool = False,
        exclude_prefixes: Optional[Iterable[str]] = None,
        exclude_regex: Optional[str] = None,
    ) -> None:
        if WorkspaceClient is None and sdk_client is None:
            raise RuntimeError("Databricks SDK not installed; install `databricks-sdk` to use discovery.")

        self.w = sdk_client or WorkspaceClient()
        self.include_views = include_views
        self.exclude_prefixes = list(exclude_prefixes) if exclude_prefixes else None
        self.exclude_re = re.compile(exclude_regex) if exclude_regex else None

        # Handle SDK surface diffs across versions
        self._tables = getattr(self.w, "tables", None) or getattr(getattr(self.w, "unity_catalog", None), "tables", None)
        self._schemas = getattr(self.w, "schemas", None) or getattr(getattr(self.w, "unity_catalog", None), "schemas", None)
        self._catalogs = getattr(self.w, "catalogs", None) or getattr(getattr(self.w, "unity_catalog", None), "catalogs", None)
        if not self._tables or not self._schemas:
            raise RuntimeError("SDK client doesn’t expose Unity Catalog tables/schemas on this workspace client.")

    # ---------------------------
    # Public API
    # ---------------------------

    def discover_pipeline_tables(
        self,
        pipeline_name: str,
        *,
        page_size: int = 250,
        empty_page_tolerance: int = 2,
    ) -> List[str]:
        """
        Tables emitted by a DLT pipeline (derived from event logs, latest update only).
        Returns sorted list of fully qualified table names when available in events;
        if events only surface flow names, you'll get those names verbatim.
        """
        print(f"[INFO] Using WorkspaceClient: {type(self.w).__name__}")

        pipelines = list(self.w.pipelines.list_pipelines())
        pl = next((p for p in pipelines if p.name == pipeline_name), None)
        if not pl:
            raise RuntimeError(f"Pipeline '{pipeline_name}' not found via SDK.")

        if not pl.latest_updates:
            raise RuntimeError(f"Pipeline '{pipeline_name}' has no updates recorded.")
        latest_update = pl.latest_updates[0].update_id
        print(f"[INFO] Latest update ID: {latest_update} (pipeline: {pipeline_name})")

        try:
            events = self.w.pipelines.list_pipeline_events(pipeline_id=pl.pipeline_id, max_results=page_size)
        except Exception as e:
            raise RuntimeError(f"Failed to list pipeline events for '{pipeline_name}': {e}")

        tables, empty_pages = set(), 0
        it = iter(events)
        while True:
            page = []
            try:
                for _ in range(page_size):
                    page.append(next(it))
            except StopIteration:
                pass

            page_tables = {
                getattr(ev.origin, "flow_name", None)
                for ev in page
                if getattr(ev.origin, "update_id", None) == latest_update and getattr(ev.origin, "flow_name", None)
            }
            page_tables.discard(None)

            if page_tables:
                tables |= page_tables
                empty_pages = 0
                print(f"[DEBUG] +{len(page_tables)} table name(s) in page")
            else:
                empty_pages += 1
                print(f"[DEBUG] no tables in page, empty_pages={empty_pages}")

            if empty_pages >= empty_page_tolerance or not page:
                break

        found = sorted(tables)
        if not found:
            raise RuntimeError(f"No output tables found for pipeline '{pipeline_name}' via event logs.")
        print(f"[INFO] Found {len(found)} table(s) for pipeline '{pipeline_name}'")
        return found

    def discover_schema_tables(self, schema_fqn: str) -> List[str]:
        """
        All tables under a schema (catalog.schema). SDK control-plane only.
        Respects include_views/exclude_* configured on the instance.
        """
        catalog, schema = _parse_schema_fqn(schema_fqn)
        self._assert_schema_exists(catalog, schema)

        out = self._list_tables_for_schema(catalog, schema)
        if not out:
            raise RuntimeError(f"No tables found under schema {catalog}.{schema} (include_views={self.include_views}).")
        out = sorted(set(out))
        print(f"[INFO] {catalog}.{schema}: {len(out)} table(s)")
        return out

    def discover_catalog_tables(self, catalog: str) -> List[str]:
        """
        All tables across all schemas in a catalog. SDK control-plane only.
        Calls discover_schema_tables() for each schema to avoid code duplication.
        """
        self._assert_catalog_exists(catalog)
        schemas = self._list_schema_names(catalog)
        if not schemas:
            raise RuntimeError(f"Catalog '{catalog}' has no schemas visible.")

        all_tables: List[str] = []
        for sch in schemas:
            try:
                all_tables.extend(self.discover_schema_tables(f"{catalog}.{sch}"))
            except Exception as e:
                print(f"[WARN] skipping {catalog}.{sch}: {e}")

        all_tables = sorted(set(all_tables))
        if not all_tables:
            raise RuntimeError(f"No tables found under catalog {catalog} (include_views={self.include_views}).")
        print(f"[INFO] Catalog {catalog}: {len(all_tables)} table(s) across {len(schemas)} schema(s)")
        return all_tables

    # ---------------------------
    # Internals (shared)
    # ---------------------------

    def _assert_catalog_exists(self, catalog: str) -> None:
        try:
            if self._catalogs is not None:
                if not any(c.name == catalog for c in self._catalogs.list()):  # type: ignore[attr-defined]
                    raise RuntimeError(f"Catalog not found: {catalog}")
            else:
                # Some SDKs lack catalogs svc; check schemas instead
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

    def _excluded(self, tbl_name: str) -> bool:
        if self.exclude_prefixes and any(tbl_name.startswith(p) for p in self.exclude_prefixes):
            return True
        if self.exclude_re and self.exclude_re.search(tbl_name):
            return True
        return False

    def _list_tables_for_schema(self, catalog: str, schema: str) -> List[str]:
        out: List[str] = []
        try:
            for t in self._tables.list(catalog_name=catalog, schema_name=schema):  # type: ignore[attr-defined]
                tname = getattr(t, "name", None)
                ttype = getattr(t, "table_type", None)
                if not tname:
                    continue
                if (not self.include_views) and _is_view(ttype):
                    continue
                if self._excluded(tname):
                    continue
                out.append(f"{t.catalog_name}.{t.schema_name}.{tname}")
        except Exception as e:
            raise RuntimeError(f"Failed listing tables for {catalog}.{schema}: {e}")
        return out