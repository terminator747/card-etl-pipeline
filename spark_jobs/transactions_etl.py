from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    to_timestamp,
    year,
    month,
    sum as _sum,
    count as _count,
)

import os


def get_spark_session(app_name: str = "CardTransactionsETL") -> SparkSession:
    """
    Create or get a local SparkSession.
    Runs in local mode for demo.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")  # use all local cores
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def extract_transactions(spark: SparkSession, raw_path: str):
    """
    Extract step: read CSV from data/raw.
    """
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(raw_path)
    )
    return df


def transform_transactions(df):
    """
    Transform step:
    - Cast types
    - Add date features
    - Amount category
    - Simple aggregations
    """

    # 1. Parse timestamp
    df = df.withColumn(
        "transaction_ts",
        to_timestamp(col("transaction_ts"), "yyyy-MM-dd HH:mm:ss")
    )

    # 2. Extract year & month
    df = df.withColumn("year", year(col("transaction_ts")))
    df = df.withColumn("month", month(col("transaction_ts")))

    # 3. Cast amount to double
    df = df.withColumn("amount", col("amount").cast("double"))

    # 4. Cast is_fraud to int (0/1)
    df = df.withColumn("is_fraud", col("is_fraud").cast("int"))

    # 5. Amount category
    df = df.withColumn(
        "amount_category",
        when(col("amount") < 500, "Low")
        .when((col("amount") >= 500) & (col("amount") < 2000), "Medium")
        .otherwise("High")
    )

    # 6. Aggregation: total amount per customer per month
    agg_customer_month = (
        df.groupBy("customer_id", "year", "month")
        .agg(
            _sum("amount").alias("total_spent"),
            _count("*").alias("transaction_count")
        )
    )

    # 7. Aggregation: fraud rate by city
    agg_fraud_city = (
        df.groupBy("city")
        .agg(
            _count("*").alias("total_txn"),
            _sum("is_fraud").alias("fraud_count")
        )
        .withColumn("fraud_rate", col("fraud_count") / col("total_txn"))
    )

    return df, agg_customer_month, agg_fraud_city


def load_curated(
    df_clean,
    agg_customer_month,
    agg_fraud_city,
    curated_base_path: str
):
    """
    Load step: write curated data as Parquet.
    """

    clean_path = os.path.join(curated_base_path, "clean_transactions")
    cust_month_path = os.path.join(curated_base_path, "customer_monthly_spend")
    fraud_city_path = os.path.join(curated_base_path, "fraud_by_city")

    (
        df_clean
        .coalesce(1)  # single file for demo
        .write
        .mode("overwrite")
        .parquet(clean_path)
    )

    (
        agg_customer_month
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(cust_month_path)
    )

    (
        agg_fraud_city
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(fraud_city_path)
    )

    print(f"✅ Wrote curated data to: {curated_base_path}")


def main():
    # base_dir = project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_path = os.path.join(base_dir, "data", "raw", "transactions_sample.csv")
    curated_path = os.path.join(base_dir, "data", "curated")

    spark = get_spark_session()

    print("🔹 Extracting data...")
    df_raw = extract_transactions(spark, raw_path)

    print("🔹 Transforming data...")
    df_clean, agg_customer_month, agg_fraud_city = transform_transactions(df_raw)

    print("🔹 Loading curated outputs...")
    load_curated(df_clean, agg_customer_month, agg_fraud_city, curated_path)

    spark.stop()
    print("✅ ETL finished successfully.")


if __name__ == "__main__":
    main()
