# src/layker/utils/table.py
from __future__ import annotations

from typing import Tuple, List, Optional
from pyspark.sql import SparkSession, DataFrame
from layker.utils.printer import Print

__all__ = [
    "table_exists", "refresh_table", "spark_sql_to_df", "spark_df_to_rows",
    "is_view",
    "is_fully_qualified_table_name",
    "parse_fully_qualified_table_name",
    "parse_catalog_schema_fqn",
    "qualify_table_name",
    "qualify_with_schema_fqn",
    "ensure_fully_qualified",
]

# -------- Spark helpers --------

def table_exists(spark: SparkSession, fully_qualified_table: str) -> bool:
    try:
        exists: bool = spark.catalog.tableExists(fully_qualified_table)
        return bool(exists)
    except Exception as e:
        print(f"{Print.ERROR}Exception in table_exists({fully_qualified_table}): {e}")
        return False

def refresh_table(spark: SparkSession, fq: str) -> None:
    try:
        spark.sql(f"REFRESH TABLE {fq}")
    except Exception as e:
        msg = str(e)
        if "NOT_SUPPORTED_WITH_SERVERLESS" in msg.upper() or "SERVERLESS" in msg.upper():
            print(f"{Print.WARN}Skipping REFRESH TABLE on serverless for {fq}.")
            return
        raise

def spark_sql_to_df(spark: SparkSession, sql: str) -> DataFrame:
    try:
        return spark.sql(sql)
    except Exception as e:
        print(f"{Print.ERROR}spark_sql_to_df failed: {e}\nSQL: {sql}")
        raise

def spark_df_to_rows(df: DataFrame) -> List[dict]:
    try:
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        print(f"{Print.ERROR}spark_df_to_rows failed: {e}")
        raise

# -------- UC identity & FQN helpers --------

def is_view(table_type: Optional[str]) -> bool:
    t = (table_type or "").upper()
    return t in {"VIEW", "MATERIALIZED_VIEW"}

def is_fully_qualified_table_name(name: str) -> bool:
    return isinstance(name, str) and name.count(".") == 2

def parse_fully_qualified_table_name(fq_table: str) -> Tuple[str, str, str]:
    if not isinstance(fq_table, str):
        print(f"{Print.ERROR}fq_table must be a string, got {type(fq_table).__name__}")
        raise TypeError("fq_table must be a string.")
    parts = fq_table.split(".")
    if len(parts) != 3:
        print(f"{Print.ERROR}Expected catalog.schema.table, got: {fq_table!r}")
        raise ValueError("Expected catalog.schema.table format.")
    return parts[0], parts[1], parts[2]

def parse_catalog_schema_fqn(schema_fqn: str) -> Tuple[str, str]:
    if not isinstance(schema_fqn, str):
        print(f"{Print.ERROR}schema_fqn must be a string, got {type(schema_fqn).__name__}")
        raise TypeError("schema_fqn must be a string.")
    parts = schema_fqn.split(".")
    if len(parts) != 2:
        print(f"{Print.ERROR}Expected 'catalog.schema', got: {schema_fqn!r}")
        raise ValueError(f"Expected 'catalog.schema', got: {schema_fqn!r}")
    return parts[0], parts[1]

def qualify_table_name(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"

def qualify_with_schema_fqn(schema_fqn: str, name: str) -> str:
    if is_fully_qualified_table_name(name):
        return name
    cat, sch = parse_catalog_schema_fqn(schema_fqn)
    return qualify_table_name(cat, sch, name)

def ensure_fully_qualified(
    name: str,
    *,
    default_schema_fqn: Optional[str] = None,
    default_catalog: Optional[str] = None,
    default_schema: Optional[str] = None,
) -> str:
    if is_fully_qualified_table_name(name):
        return name
    if default_schema_fqn:
        return qualify_with_schema_fqn(default_schema_fqn, name)
    if default_catalog and default_schema:
        return qualify_table_name(default_catalog, default_schema, name)
    raise ValueError(f"Not fully qualified and no defaults provided: {name!r}")