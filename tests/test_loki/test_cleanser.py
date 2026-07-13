import pytest
from pyspark.sql import SparkSession

from loki.cleanser import cleanse_customers_data, cleanse_orders_data, cleanse_products_data
from loki.spark_config import get_spark_session


@pytest.fixture(scope="module")
def spark_session() -> SparkSession:
    spark = get_spark_session()
    yield spark
    spark.stop()


def test_cleanse_customers_data(spark_session: SparkSession) -> None:
    # given
    raw_data = [
        ("12345", "United Kingdom"),
        ("12346", "France"),
        (None, "Germany"),  # Null CustomerID to be filtered
        ("12347", "Spain"),
        ("12345", "United Kingdom"),  # Duplicate
    ]

    raw_df = spark_session.createDataFrame(
        raw_data,
        ["CustomerID", "Country"]
    )

    # when
    cleansed_df = cleanse_customers_data(raw_df)

    # then
    assert cleansed_df.count() == 3, "Should have 3 unique non-null customers"
    assert cleansed_df.filter(cleansed_df.CustomerID.isNull()).count() == 0, "Should have no null CustomerIDs"

    customer_ids = [row.CustomerID for row in cleansed_df.select("CustomerID").collect()]
    assert "12345" in customer_ids
    assert "12346" in customer_ids
    assert "12347" in customer_ids


def test_cleanse_orders_data(spark_session: SparkSession) -> None:
    # given
    raw_data = [
        ("536365", "85123A", 6, "2010-12-01 08:26:00", "17850"),
        ("536365", "71053", 6, "2010-12-01 08:26:00", "17850"),
        (None, "84406B", 8, "2010-12-01 08:26:00", "17850"),  # Null InvoiceNo to be filtered
        ("536367", None, 12, "2010-12-01 08:34:00", "13047"),  # Null StockCode to be filtered
        ("536367", "84879", 12, "2010-12-01 08:34:00", None),  # Null CustomerID to be filtered
        ("536368", "22752", 2, "2010-12-01 08:34:00", "13047"),
        ("536365", "85123A", 6, "2010-12-01 08:26:00", "17850"),  # Duplicate
    ]

    raw_df = spark_session.createDataFrame(
        raw_data,
        ["InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "CustomerID"]
    )

    # when
    cleansed_df = cleanse_orders_data(raw_df)

    # then
    assert cleansed_df.count() == 3, "Should have 3 unique non-null orders"
    assert cleansed_df.filter(cleansed_df.InvoiceNo.isNull()).count() == 0, "Should have no null InvoiceNo"
    assert cleansed_df.filter(cleansed_df.StockCode.isNull()).count() == 0, "Should have no null StockCode"
    assert cleansed_df.filter(cleansed_df.CustomerID.isNull()).count() == 0, "Should have no null CustomerID"

    invoice_nos = [row.InvoiceNo for row in cleansed_df.select("InvoiceNo").distinct().collect()]
    assert "536365" in invoice_nos
    assert "536368" in invoice_nos


def test_cleanse_products_data(spark_session: SparkSession) -> None:
    # given
    raw_data = [
        ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2.55),
        ("71053", "WHITE METAL LANTERN", 3.39),
        (None, "CREAM CUPID HEARTS COAT HANGER", 2.75),  # Null StockCode to be filtered
        ("84406B", None, 2.75),  # Null Description to be filtered
        ("22752", "SET 7 BABUSHKA NESTING BOXES", 7.65),
        ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2.55),  # Duplicate based on StockCode
        ("21730", "GLASS STAR FROSTED T-LIGHT HOLDER", 4.25),
    ]

    raw_df = spark_session.createDataFrame(
        raw_data,
        ["StockCode", "Description", "UnitPrice"]
    )

    # when
    cleansed_df = cleanse_products_data(raw_df)

    # then
    assert cleansed_df.count() == 5, "Should have 5 non-null products"
    assert cleansed_df.filter(cleansed_df.StockCode.isNull()).count() == 0, "Should have no null StockCode"
    assert cleansed_df.filter(cleansed_df.Description.isNull()).count() == 0, "Should have no null Description"

    stock_codes = [row.StockCode for row in cleansed_df.select("StockCode").collect()]
    assert "85123A" in stock_codes
    assert "71053" in stock_codes
    assert "22752" in stock_codes
    assert "21730" in stock_codes
