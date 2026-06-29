from dataclasses import dataclass
from functools import reduce
from operator import or_
from typing import Dict, List, Tuple

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, NumericType


@dataclass
class ValidationSummary:
    total_records: int
    valid_records: int
    invalid_records: int
    duplicates_removed: int


class Validator:
    def validate(self, dataframe: DataFrame) -> Tuple[DataFrame, DataFrame, ValidationSummary]:
        logger.info("Starting validation")
        total_records = dataframe.count()
        deduplicated = dataframe.dropDuplicates()
        duplicates_removed = total_records - deduplicated.count()

        error_conditions, error_flags = self._build_error_conditions(dataframe)
        if error_conditions:
            invalid_rows = deduplicated.filter(reduce(or_, error_conditions))
        else:
            invalid_rows = dataframe.limit(0)

        valid_rows = deduplicated.exceptAll(invalid_rows)
        invalid_records = invalid_rows.count()
        valid_records = valid_rows.count()

        logger.info(
            "Validation completed: total={} valid={} invalid={} duplicates={}",
            total_records,
            valid_records,
            invalid_records,
            duplicates_removed,
        )

        invalid_rows = self._attach_error_flags(invalid_rows, error_flags)

        return valid_rows, invalid_rows, ValidationSummary(
            total_records=total_records,
            valid_records=valid_records,
            invalid_records=invalid_records,
            duplicates_removed=duplicates_removed,
        )

    def _build_error_conditions(self, dataframe: DataFrame):
        error_conditions = []
        error_flags = []

        null_condition = reduce(
            or_,
            [F.col(column).isNull() for column in dataframe.columns],
            F.lit(False),
        )
        error_conditions.append(null_condition)
        error_flags.append(("Null values", null_condition))

        date_columns = [column for column in dataframe.columns if "date" in column.lower()]
        for column in date_columns:
            parsed_date = F.to_date(F.col(column))
            invalid_date = F.col(column).isNotNull() & parsed_date.isNull()
            error_conditions.append(invalid_date)
            error_flags.append(("Invalid dates", invalid_date))

        # Identify amount-like columns by specific tokens (avoid matching 'transaction' which can match dates)
        amount_columns = [column for column in dataframe.columns if any(token in column.lower() for token in ("amount", "price", "total"))]
        for column in amount_columns:
            col_type = dataframe.schema[column].dataType
            # Numeric columns: direct negative check
            if isinstance(col_type, NumericType):
                negative = F.col(column) < F.lit(0)
                error_conditions.append(negative)
                error_flags.append(("Negative amounts", negative))
            # String columns: validate numeric format first, then check negative by casting
            elif isinstance(col_type, StringType):
                invalid_numeric = F.col(column).isNotNull() & ~F.col(column).rlike(r"^-?\d+(\.\d+)?$")
                error_conditions.append(invalid_numeric)
                error_flags.append(("Invalid numeric data", invalid_numeric))

                negative_string = (
                    F.col(column).isNotNull()
                    & F.col(column).rlike(r"^-?\d+(\.\d+)?$")
                    & (F.col(column).cast("double") < F.lit(0))
                )
                error_conditions.append(negative_string)
                error_flags.append(("Negative amounts", negative_string))
            else:
                # Skip negative checks for unsupported types (e.g., Timestamp)
                continue

        customer_columns = [column for column in dataframe.columns if "customer" in column.lower() or "cust" in column.lower()]
        for column in customer_columns:
            missing_customer = F.col(column).isNull() | (F.trim(F.col(column)) == "")
            error_conditions.append(missing_customer)
            error_flags.append(("Missing customer id", missing_customer))

        return error_conditions, error_flags

    @staticmethod
    def _attach_error_flags(invalid_rows: DataFrame, error_flags: List[tuple]) -> DataFrame:
        if invalid_rows.rdd.isEmpty():
            return invalid_rows

        annotations = [F.when(condition, label) for label, condition in error_flags]
        invalid_rows = invalid_rows.withColumn("validation_errors", F.array_join(F.array(*annotations), "; "))
        invalid_rows = invalid_rows.cache()
        invalid_rows.show(1, truncate=False)
        return invalid_rows
