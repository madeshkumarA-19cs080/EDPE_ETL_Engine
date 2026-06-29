from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from config import SPARK_APP_NAME, SPARK_OPTIONS
from database.connector import DatabaseConnector


class Extractor:
    def __init__(self, connector: DatabaseConnector, table_name: str):
        self.connector = connector
        self.table_name = table_name
        self.spark = self._build_spark_session()

    def _build_spark_session(self) -> SparkSession:
        # Use the exact working Spark initialization from test_spark_postgres.py
        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("Test")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.7")
            .getOrCreate()
        )

        # Diagnostics
        logger.info("Spark Version: %s", spark.version)
        logger.info("Spark Master: %s", spark.sparkContext.master)
        logger.info("Loaded Packages: %s", spark.conf.get("spark.jars.packages", ""))

        return spark

    def read_table(self) -> DataFrame:
        options = self.connector.jdbc_options(self.table_name)
        logger.info("ENTERING read_table()")
        logger.info("Starting extraction from %s table %s", self.connector.dialect(), self.table_name)
        logger.debug("JDBC options (no password): %s", {k: v for k, v in options.items() if k != "password"})

        # Follow the exact read pattern from test_spark_postgres.py
        dataframe = (
            self.spark.read.format("jdbc")
            .option("url", options.get("url"))
            .option("dbtable", options.get("dbtable"))
            .option("user", options.get("user"))
            .option("password", options.get("password"))
            .option("driver", options.get("driver"))
            .load()
        )
        logger.info("Completed extraction: %s rows available in Spark schema", dataframe.count())
        return dataframe
