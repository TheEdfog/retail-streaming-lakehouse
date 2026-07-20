import argparse


def order_schema():
    from pyspark.sql.types import (
        DecimalType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("event_time", TimestampType(), False),
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("store_id", IntegerType(), False),
            StructField("amount", DecimalType(12, 2), False),
            StructField("currency", StringType(), False),
            StructField("status", StringType(), False),
            StructField("schema_version", IntegerType(), False),
        ]
    )


def parse_orders(kafka_rows):
    """Parse once, quarantine invalid JSON and keep replay metadata."""
    from pyspark.sql import functions as F

    decoded = kafka_rows.select(
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_time"),
    )
    return decoded.withColumn("event", F.from_json("raw_value", order_schema()))


def _valid_event_condition():
    from pyspark.sql import functions as F

    return (
        F.col("event").isNotNull()
        & (F.col("event.amount") > 0)
        & F.col("event.currency").rlike("^[A-Z]{3}$")
        & F.col("event.status").isin("created", "paid", "cancelled")
        & (F.col("event.schema_version") == 1)
    )


def valid_orders(parsed_rows):
    return (
        parsed_rows.where(_valid_event_condition())
        .select("event.*", "topic", "partition", "offset", "kafka_time")
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["event_id"])
    )


def invalid_orders(parsed_rows):
    from pyspark.sql import functions as F

    is_valid = F.coalesce(_valid_event_condition(), F.lit(False))
    return parsed_rows.where(~is_valid).select(
        "raw_value",
        "topic",
        "partition",
        "offset",
        "kafka_time",
        F.when(F.col("event").isNull(), F.lit("malformed_json"))
        .otherwise(F.lit("contract_validation_failed"))
        .alias("reason"),
    )


def build_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("retail-streaming-lakehouse")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.lake.uri", "http://localhost:19120/api/v1")
        .config("spark.sql.catalog.lake.ref", "main")
        .config("spark.sql.catalog.lake.warehouse", "s3://warehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:19000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:19092")
    parser.add_argument("--topic", default="retail.orders.v1")
    parser.add_argument("--checkpoint", default=".state/orders")
    args = parser.parse_args()

    spark = build_spark()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.retail")
    spark.sql(
        """CREATE TABLE IF NOT EXISTS lake.retail.orders (
        event_id string, event_time timestamp, order_id string, customer_id string,
        store_id int, amount decimal(12,2), currency string, status string,
        schema_version int, topic string, partition int, offset bigint, kafka_time timestamp
        ) USING iceberg PARTITIONED BY (days(event_time), store_id)"""
    )
    kafka_rows = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = parse_orders(kafka_rows)
    good = valid_orders(parsed)
    bad = invalid_orders(parsed)

    accepted_query = (
        good.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", f"{args.checkpoint}/valid")
        .toTable("lake.retail.orders")
    )
    quarantine_query = (
        bad.writeStream.format("json")
        .option("path", "data/quarantine")
        .option("checkpointLocation", f"{args.checkpoint}/quarantine")
        .start()
    )
    try:
        spark.streams.awaitAnyTermination()
    finally:
        accepted_query.stop()
        quarantine_query.stop()


if __name__ == "__main__":
    main()
