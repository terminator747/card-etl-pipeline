import pytest

from pyspark.sql import SparkSession
from spark_jobs.transactions_etl import transform_transactions


@pytest.fixture(scope="session")
def spark():

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("CardETLTests")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()


def test_transform_transactions(spark):

    data = [
        (
            "C001",
            "2026-01-10 10:00:00",
            200.0,
            0,
            "Mumbai"
        ),
        (
            "C001",
            "2026-01-11 11:00:00",
            1500.0,
            1,
            "Mumbai"
        )
    ]

    columns = [
        "customer_id",
        "transaction_ts",
        "amount",
        "is_fraud",
        "city"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    clean, customer_month, fraud_city = (
        transform_transactions(df)
    )

    assert clean.count() == 2
    assert customer_month.count() == 1
    assert fraud_city.count() == 1


def test_amount_categories(spark):

    data = [
        (
            "C001",
            "2026-01-10 10:00:00",
            100.0,
            0,
            "Mumbai"
        ),
        (
            "C002",
            "2026-01-10 10:00:00",
            1000.0,
            0,
            "Delhi"
        ),
        (
            "C003",
            "2026-01-10 10:00:00",
            5000.0,
            1,
            "Pune"
        )
    ]

    columns = [
        "customer_id",
        "transaction_ts",
        "amount",
        "is_fraud",
        "city"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    clean, _, _ = transform_transactions(df)

    categories = {
        row["amount_category"]
        for row in clean.collect()
    }

    assert "Low" in categories
    assert "Medium" in categories
    assert "High" in categories