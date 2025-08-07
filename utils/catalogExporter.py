# utils/catalogExporter.py

"""
Module: catalogExporter.py
Description: Provides CsvExporter for exporting Spark tables (or paths) to a single CSV,
             with date‑range filtering (from target_date through today).
"""

import os
import re
import shutil
import multiprocessing
from datetime import datetime, date
from typing import Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException


class CsvExporter:
    """
    CsvExporter: Exports Spark tables to a single CSV file with optional date‑range filtering.

    Public API:
      • export_table(table_name, output_file, date_column=None, target_date=None)
    """

    @classmethod
    def export_table(
        cls,
        table_name: str,
        output_file: str,
        date_column: Optional[str] = None,
        target_date: Optional[str] = None
    ) -> None:
        """One‑call export: builds SparkSession and runs the export."""
        if date_column and not target_date:
            raise ValueError("Must supply target_date when date_column is provided.")

        spark = (
            SparkSession.builder
            .appName("CsvExporter")
            .getOrCreate()
        )
        exporter = cls(spark)
        exporter._run_export(table_name, output_file, date_column, target_date)

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def _run_export(
        self,
        table_name: str,
        output_file: str,
        date_column: Optional[str],
        target_date: Optional[str]
    ) -> None:
        """Internal: load, optional filter, write parts, merge, clean up."""
        try:
            df: DataFrame = self.spark.table(table_name)
        except AnalysisException as exc:
            raise ValueError(f"Table not found: {exc}")

        if date_column:
            df = self._apply_date_filter(df, date_column, target_date)  # type: ignore

        tmp_dir = output_file.rstrip(".csv") + "_tmp"
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)

        # write in parallel with header on each part
        df.write.mode("overwrite").option("header", True).csv(tmp_dir)

        # merge parts into single CSV
        self._merge_csv_parts(tmp_dir, output_file)
        shutil.rmtree(tmp_dir)

        print(f"[SUCCESS] Exported {table_name} → {output_file}")

    def _apply_date_filter(
        self,
        df: DataFrame,
        date_column: str,
        target_date_str: str
    ) -> DataFrame:
        """
        Filter df so date_column ∈ [target_date, today].
        target_date_str must be 'YYYY', 'YYYY-MM', or 'YYYY-MM-DD'.
        """
        # determine lower bound
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", target_date_str):
            lower = datetime.strptime(target_date_str, "%Y-%m-%d").date()
        elif re.fullmatch(r"\d{4}-\d{2}", target_date_str):
            y, m = map(int, target_date_str.split("-"))
            lower = date(y, m, 1)
        elif re.fullmatch(r"\d{4}", target_date_str):
            y = int(target_date_str)
            lower = date(y, 1, 1)
        else:
            raise ValueError("target_date must be 'YYYY', 'YYYY-MM' or 'YYYY-MM-DD'")

        upper = date.today()
        print(f"[INFO] Filtering {date_column} from {lower} through {upper}")

        # cast column to date and filter
        col_dt = F.to_date(F.col(date_column))
        return df.filter(col_dt.between(lower.isoformat(), upper.isoformat()))

    @staticmethod
    def _read_part(args: Tuple[int, str, bool]) -> Tuple[int, bytes]:
        idx, path, skip_header = args
        with open(path, "rb") as f:
            if skip_header:
                f.readline()
            return idx, f.read()

    def _merge_csv_parts(self, source_dir: str, dest_file: str) -> None:
        """Combine Spark part-*.csv into one CSV via a thread pool."""
        if not os.path.isdir(source_dir):
            raise FileNotFoundError(f"Source not found: {source_dir}")

        parts = sorted(
            f for f in os.listdir(source_dir)
            if f.startswith("part-") and f.endswith(".csv")
        )
        if not parts:
            raise FileNotFoundError(f"No CSV parts in {source_dir}")

        tasks = [(i, os.path.join(source_dir, p), i > 0)
                 for i, p in enumerate(parts)]
        workers = min(multiprocessing.cpu_count(), len(tasks))
        with ThreadPoolExecutor(max_workers=workers) as exe:
            results = list(exe.map(CsvExporter._read_part, tasks))

        os.makedirs(os.path.dirname(dest_file), exist_ok=True)
        with open(dest_file, "wb") as out:
            for idx, chunk in sorted(results, key=lambda x: x[0]):
                out.write(chunk)