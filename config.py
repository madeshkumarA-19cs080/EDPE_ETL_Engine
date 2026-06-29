from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
OUTPUT_DIR = BASE_DIR / "output"
REPORT_DIR = BASE_DIR / "reports"
LOG_FILE = LOG_DIR / "edpe.log"
for path in (LOG_DIR, OUTPUT_DIR, REPORT_DIR):
    path.mkdir(parents=True, exist_ok=True)

SUPPORTED_DATABASES = {
    "sqlserver": "SQL Server",
    "postgres": "PostgreSQL",
}

OUTPUT_FORMATS = ["csv", "parquet", "avro"]

SPARK_APP_NAME = "Enterprise Data Processing Engine"
SPARK_OPTIONS = {
    "spark.driver.memory": "2g",
    "spark.sql.shuffle.partitions": "8",
}

# JDBC drivers are loaded via Spark packages; local jar patterns removed.

TEST_QUERY = "SELECT 1"
