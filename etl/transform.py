from typing import List

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, TimestampType


class Transformer:
    def transform(self, dataframe: DataFrame) -> DataFrame:
        logger.info("Starting transformation")

        dataframe = self._trim_strings(dataframe)
        dataframe = self._uppercase_branch_fields(dataframe)
        dataframe = self._standardize_currency_fields(dataframe)
        dataframe = self._convert_date_fields(dataframe)
        dataframe = self._add_derived_date_columns(dataframe)

        logger.info("Transformation completed")
        return dataframe

    def _trim_strings(self, dataframe: DataFrame) -> DataFrame:
        for field in dataframe.schema.fields:
            if isinstance(field.dataType, StringType):
                dataframe = dataframe.withColumn(field.name, F.trim(F.col(field.name)))
        return dataframe

    def _uppercase_branch_fields(self, dataframe: DataFrame) -> DataFrame:
        branch_columns = [column for column in dataframe.columns if "branch" in column.lower()]
        for column in branch_columns:
            dataframe = dataframe.withColumn(column, F.upper(F.col(column)))
        return dataframe

    def _standardize_currency_fields(self, dataframe: DataFrame) -> DataFrame:
        currency_columns = [column for column in dataframe.columns if "currency" in column.lower() or "amount" in column.lower()]
        for column in currency_columns:
            dataframe = dataframe.withColumn(
                column,
                F.upper(F.regexp_replace(F.col(column).cast(StringType()), r"[^A-Z0-9.\-]", "")),
            )
        return dataframe

    def _convert_date_fields(self, dataframe: DataFrame) -> DataFrame:
        date_columns = [field.name for field in dataframe.schema.fields if "date" in field.name.lower() or isinstance(field.dataType, (DateType, TimestampType))]
        for column in date_columns:
            dataframe = dataframe.withColumn(column, F.to_date(F.col(column)))
        return dataframe

    def _add_derived_date_columns(self, dataframe: DataFrame) -> DataFrame:
        date_column = next((column for column in dataframe.columns if "date" in column.lower()), None)
        if not date_column:
            return dataframe

        dataframe = (
            dataframe
            .withColumn("Year", F.year(F.col(date_column)))
            .withColumn("Month", F.month(F.col(date_column)))
            .withColumn("Quarter", F.quarter(F.col(date_column)))
            .withColumn("Week", F.weekofyear(F.col(date_column)))
            .withColumn("Day", F.dayofmonth(F.col(date_column)))
        )
        return dataframe
