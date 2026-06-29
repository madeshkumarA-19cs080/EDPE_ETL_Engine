import glob
import re
import shutil
from pathlib import Path
from typing import Optional

from loguru import logger
from pyspark.sql import DataFrame

from config import OUTPUT_DIR, REPORT_DIR


class DataWriter:
    def __init__(self) -> None:
        self.output_dir = Path(OUTPUT_DIR)
        self.report_dir = Path(REPORT_DIR)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _normalize_name(name: str) -> str:
        safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", name)
        return safe_name.strip("_") or "edpe_output"

    def write_data(self, dataframe: DataFrame, output_format: str, base_name: str, partition_by: Optional[str] = None) -> Path:
        safe_name = self._normalize_name(base_name)
        target = self.output_dir / f"{safe_name}_{output_format}"
        logger.info("Writing {} output to {}", output_format.upper(), target)

        if output_format == "csv":
            dataframe.write.mode("overwrite").option("header", "true").csv(str(target))
        elif output_format == "parquet":
            writer = dataframe.write.mode("overwrite").option("compression", "snappy")
            if partition_by:
                writer = writer.partitionBy(partition_by)
            writer.parquet(str(target))
        elif output_format == "avro":
            writer = dataframe.write.mode("overwrite").format("avro")
            if partition_by:
                writer = writer.partitionBy(partition_by)
            writer.save(str(target))
        else:
            raise ValueError(f"Unsupported format: {output_format}")

        return target

    def write_error_report(self, dataframe: DataFrame, report_name: str) -> Path:
        safe_name = self._normalize_name(report_name)
        target_dir = self.report_dir / f"{safe_name}_error_report"
        output_file = self.report_dir / f"{safe_name}_error_report.csv"
        logger.info("Writing error report to {}", output_file)
        if dataframe.rdd.isEmpty():
            output_file.write_text("No validation errors detected.\n", encoding="utf-8")
            return output_file

        if output_file.exists():
            output_file.unlink()
        if target_dir.exists():
            shutil.rmtree(target_dir)

        dataframe.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(target_dir))
        part_files = glob.glob(str(target_dir / "part-*.csv"))
        if not part_files:
            raise RuntimeError("Error report export failed: no output file created")

        shutil.move(part_files[0], output_file)
        shutil.rmtree(target_dir)
        return output_file

    def write_execution_report(self, summary: dict, report_name: str) -> Path:
        safe_name = self._normalize_name(report_name)
        path = self.report_dir / f"{safe_name}_execution_report.txt"
        logger.info("Writing execution report to {}", path)
        content = [f"{key}: {value}" for key, value in summary.items()]
        Path(path).write_text("\n".join(content), encoding="utf-8")
        return path
