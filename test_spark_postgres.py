from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Test")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.7"
    )
    .getOrCreate()
)

df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/edpe_db")
    .option("dbtable", "banking.transactions")
    .option("user", "postgres")
    .option("password", "M@123hsedam")
    .option("driver", "org.postgresql.Driver")
    .load()
)

df.show(5)