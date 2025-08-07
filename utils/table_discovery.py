# src/datalake_pipeline_expectations/package/utils/table_discovery.py

from typing import List, Optional

try:
    from databricks.sdk import WorkspaceClient
except ImportError as e:
    WorkspaceClient = None
    print(f"[ERROR] databricks.sdk import failed: {e}")

def discover_output_tables(
    pipeline_name: str,
    sdk_client: Optional[object] = None
) -> List[str]:
    """
    Uses Databricks SDK to discover all tables written by a pipeline.
    Always runs live, never caches results. Robust for production use.

    Args:
        pipeline_name (str): Name of the pipeline.
        sdk_client (WorkspaceClient, optional): Inject a WorkspaceClient (mock for tests).

    Returns:
        List[str]: Sorted list of output table names.

    Raises:
        RuntimeError: If SDK is missing, or pipeline/tables not found.
    """
    if WorkspaceClient is None and sdk_client is None:
        print("[ERROR] Databricks SDK is not installed. Table discovery requires databricks-sdk.")
        raise RuntimeError("Databricks SDK is not installed. Table discovery requires databricks-sdk.")

    w = sdk_client or WorkspaceClient()
    print(f"[INFO] Using WorkspaceClient: {type(w).__name__}")

    pipelines = list(w.pipelines.list_pipelines())
    pl = next((p for p in pipelines if p.name == pipeline_name), None)
    if not pl:
        print(f"[ERROR] Pipeline '{pipeline_name}' not found via SDK.")
        raise RuntimeError(f"Pipeline '{pipeline_name}' not found via SDK.")

    latest_update = pl.latest_updates[0].update_id
    print(f"[INFO] Using latest update ID: {latest_update} for pipeline: {pipeline_name}")

    try:
        events = w.pipelines.list_pipeline_events(pipeline_id=pl.pipeline_id, max_results=250)
    except Exception as e:
        print(f"[ERROR] Failed to list pipeline events: {e}")
        raise RuntimeError(f"Failed to list pipeline events for pipeline '{pipeline_name}': {e}")

    tables = set()
    empty_pages = 0
    buffer = []
    it = iter(events)
    while True:
        buffer.clear()
        try:
            for _ in range(250):
                buffer.append(next(it))
        except StopIteration:
            pass

        page_tables = {
            getattr(ev.origin, "flow_name", None)
            for ev in buffer
            if getattr(ev.origin, "update_id", None) == latest_update and getattr(ev.origin, "flow_name", None)
        }
        page_tables.discard(None)
        if page_tables:
            tables |= page_tables
            empty_pages = 0
            print(f"[DEBUG] Found tables in this page: {sorted(page_tables)}")
        else:
            empty_pages += 1
            print(f"[DEBUG] No tables found in this page. Empty pages count: {empty_pages}")
        if empty_pages >= 2 or not buffer:
            break

    found = sorted(tables)
    if not found:
        print(f"[ERROR] No output tables found for pipeline '{pipeline_name}' using SDK event logs.")
        raise RuntimeError(
            f"No output tables found for pipeline '{pipeline_name}' using SDK event logs."
        )
    print(f"[INFO] Found {len(found)} tables for pipeline '{pipeline_name}': {found}")
    return found
