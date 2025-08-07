# utils/deltaConfig.py

"""
Module: utils/deltaConfig.py
Description:
  This module provides the DeltaTableUpdater class, which is responsible for managing
  Delta Lake table operations within a PySpark environment. It facilitates creating Delta tables
  based on a target schema defined in a YAML configuration file (e.g., job-monitor.yaml) and supports
  reading from Delta tables, and schema management.
  Merging/upserts must now be handled by SQL in the calling notebook or class.

Author: Levi Gagne
Created Date: 2025-03-25
Last Modified: 2025-07-23
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, TimestampType, StringType, LongType, DateType, IntegerType,
    DoubleType, FloatType, BooleanType, DecimalType, BinaryType, ShortType, ByteType
)
from typing import Dict, Any, Callable

class DeltaTableUpdater:
    """
    Provides schema construction, table creation, and reading for Delta tables.
    No merge/upsert logic is included in this class; use SQL MERGE in your notebook or job monitor logic.

    - Schema is built from a YAML config with 'business' and 'plumbing' sections.
    - Table can be created if not exists.
    - Reads the table as a DataFrame.

    Args:
        spark: Active SparkSession.
        table_name: Fully qualified table name.
        yaml_config: Loaded YAML config object.
        create_if_not_exists: If True, table is created if it doesn't exist.
    """
    def __init__(self, 
                 spark: SparkSession, 
                 table_name: str, 
                 yaml_config: Any,
                 create_if_not_exists: bool = True) -> None:
        self.spark: SparkSession = spark
        self.table_name: str = table_name
        self.yaml_config = yaml_config
        self.schema: StructType = self.build_target_schema()
        self.partition_column: str = (
            self.yaml_config.config.get("app_config", {})
                .get("plumbing_columns", {})
                .get("dl_partition_column", None)
        )
        if create_if_not_exists:
            self.create_table_if_not_exists()

    def build_target_schema(self) -> StructType:
        schema_fields = []
        type_mapping: Dict[str, Callable[..., Any]] = {
            "TimestampType": TimestampType,
            "StringType": StringType,
            "LongType": LongType,
            "DateType": DateType,
            "IntegerType": IntegerType,
            "DoubleType": DoubleType,
            "FloatType": FloatType,
            "BooleanType": BooleanType,
            "DecimalType": DecimalType,
            "BinaryType": BinaryType,
            "ShortType": ShortType,
            "ByteType": ByteType
        }
        for group in ["business", "plumbing"]:
            for col_def in self.yaml_config.config.get("schema", {}).get(group, []):
                col_name = col_def.get("name")
                col_type_name = col_def.get("type")
                if col_type_name not in type_mapping:
                    raise ValueError(
                        f"Unsupported type '{col_type_name}' for column '{col_name}'. "
                        "Please add it to the type_mapping in deltaConfig.py."
                    )
                spark_type = type_mapping[col_type_name]()
                schema_fields.append(StructField(col_name, spark_type, True))
        return StructType(schema_fields)

    def create_table_if_not_exists(self) -> None:
        if not self.spark.catalog.tableExists(self.table_name):
            empty_df: DataFrame = self.spark.createDataFrame([], schema=self.schema)
            if self.partition_column:
                empty_df.write.format("delta") \
                    .partitionBy(self.partition_column) \
                    .mode("overwrite") \
                    .saveAsTable(self.table_name)
            else:
                empty_df.write.format("delta") \
                    .mode("overwrite") \
                    .saveAsTable(self.table_name)

    def read_table_as_df(self) -> DataFrame:
        return self.spark.table(self.table_name)