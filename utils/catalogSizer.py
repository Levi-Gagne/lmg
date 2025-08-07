# utils/catalogSizer.py

"""
Module: catalogSizer.py
Description: Provides SizeInspector for measuring size of filesystem paths
             (files/directories) or Spark tables, with human‑readable output.
"""

import os
import math
import multiprocessing
from typing import Union

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


class SizeInspector:
    """
    SizeInspector: Returns a message indicating the size of a path or catalog.table.

    Public API:
      • get_size(target: str) → str
    """

    @classmethod
    def get_size(cls, target: str) -> str:
        """
        Return a message: 
          - For filesystem paths: "The file, {basename} is {size}"
          - For tables:           "The table, {table_name} is {size}"
        """
        spark = (
            SparkSession.builder
            .appName("SizeInspector")
            .getOrCreate()
        )
        inspector = cls(spark)
        size_bytes = inspector._resolve_size_bytes(target)
        size_str = cls._convert_size(size_bytes)

        if "/" in target:
            name = os.path.basename(target)
            return f"The file, {name} is {size_str}"
        # assume catalog.schema.table
        return f"The table, {target} is {size_str}"

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def _resolve_size_bytes(self, target: str) -> int:
        """Detect path vs. table and compute total bytes."""
        if "/" in target or os.path.exists(target):
            return self._get_path_size_bytes(target)
        if target.count(".") == 2:
            return self._get_table_size_bytes(target)
        raise ValueError(f"Invalid target: {target!r}")

    @staticmethod
    def _get_path_size_bytes(path: str) -> int:
        """Sum bytes for a file or all files under a directory."""
        if os.path.isfile(path):
            return os.path.getsize(path)
        total = 0
        for root, _, files in os.walk(path):
            for fname in files:
                total += os.path.getsize(os.path.join(root, fname))
        return total

    def _get_table_size_bytes(self, table_name: str) -> int:
        """
        Try DESCRIBE DETAIL (Delta). Fallback: sum sizes of inputFiles().
        """
        try:
            detail = self.spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
            return int(detail["sizeInBytes"])
        except Exception:
            try:
                files = self.spark.table(table_name).inputFiles()
                total = 0
                for fpath in files:
                    local = (
                        fpath.replace("dbfs:/", "/dbfs/")
                        if fpath.startswith("dbfs:/")
                        else fpath
                    )
                    if os.path.isfile(local):
                        total += os.path.getsize(local)
                return total
            except Exception as e:
                raise ValueError(f"Unable to get table size: {e}")

    @staticmethod
    def _convert_size(size_bytes: int) -> str:
        """Convert bytes to human‑readable units."""
        if size_bytes == 0:
            return "0B"
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = 1024 ** i
        s = round(size_bytes / p, 2)
        units = ("B", "KB", "MB", "GB", "TB", "PB", "EB")
        return f"{s} {units[i]}"