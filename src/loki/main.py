import uuid
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    max as spark_max,
    last_day,
    to_date,
    add_months,
    try_to_timestamp,
    lit,
)

from loki.cleanser import cleanse_customers_data, cleanse_orders_data, cleanse_products_data
from loki.data_writer import store_data
from loki.ingestor import read_customers_data, read_orders_data, read_products_data
from loki.spark_config import get_spark_session


def main() -> None:
    base_dir: Path = Path(__file__).resolve().parents[2]
    customers_input_path: str = str(base_dir / "data" / "input" / "customers.csv")
    orders_input_path: str = str(base_dir / "data" / "input" / "orders.csv")
    products_input_path: str = str(base_dir / "data" / "input" / "products.csv")

    spark: SparkSession = get_spark_session()

    customers_raw: DataFrame = read_customers_data(customers_input_path, spark)

    orders_raw: DataFrame = (read_orders_data(orders_input_path, spark))

    products_raw: DataFrame = (read_products_data(products_input_path, spark))

    # Store raw layer
    output_raw_base_path: Path = Path(base_dir / "data" / "output" / "raw")

    customers_raw_output_path: str = str(output_raw_base_path / "customers")
    orders_raw_output_path: str = str(output_raw_base_path / "orders")
    products_raw_output_path: str = str(output_raw_base_path / "products")

    store_data(customers_raw, customers_raw_output_path, "cutomers", str(uuid.uuid4()))
    store_data(orders_raw, orders_raw_output_path, "orders", str(uuid.uuid4()))
    store_data(products_raw, products_raw_output_path, "products", str(uuid.uuid4()))

    # Checking for duplicates if there are any and what they look like

    # Cleansing - integration layer
    customers_cleansed: DataFrame = cleanse_customers_data(customers_raw)

    orders_cleansed: DataFrame = cleanse_orders_data(orders_raw)

    products_cleansed: DataFrame = cleanse_products_data(products_raw)

    # Store integration layer
    output_integration_base_path: Path = Path(base_dir / "data" / "output" / "integration")

    customers_integration_output_path: str = str(output_integration_base_path / "customers")
    orders_integration_output_path: str = str(output_integration_base_path / "orders")
    products_integration_output_path: str = str(output_integration_base_path / "products")

    store_data(customers_cleansed, customers_integration_output_path, "customers", str(uuid.uuid4()))
    store_data(orders_cleansed, orders_integration_output_path, "orders", str(uuid.uuid4()))
    store_data(products_cleansed, products_integration_output_path, "products", str(uuid.uuid4()))

    # Consumption optimization - curating layer
    output_curated_base_path: Path = Path(base_dir / "data" / "output" / "curated")

    customers_curated_output_path: str = str(output_curated_base_path / "dim_customers")
    orders_curated_output_path: str = str(output_curated_base_path / "dim_orders")
    products_curated_output_path: str = str(output_curated_base_path / "dim_products")

    store_data(customers_cleansed, customers_curated_output_path, "dim_customers", str(uuid.uuid4()))
    store_data(orders_cleansed, orders_curated_output_path, "dim_orders", str(uuid.uuid4()))
    store_data(products_cleansed, products_curated_output_path, "dim_products", str(uuid.uuid4()))

    print("Top ten countries with the most number of customers")
    (customers_cleansed.groupby(col("Country"))
     .count()
     .withColumnRenamed("count", "CustomerNumber")
     .orderBy(col("CustomerNumber").desc())
     .show(10))

    print("Revenue distribution by country")
    revenue_distribution_by_country: DataFrame = (orders_raw
                                                  .join(products_raw, on="StockCode", how="inner")
                                                  .join(customers_raw, on="CustomerID")
                                                  .withColumn("TotalRevenue", col("UnitPrice") * col("Quantity"))
                                                  .groupBy(col("Country"))
                                                  .agg({"TotalRevenue": "sum"})
                                                  .withColumnRenamed("sum(TotalRevenue)", "Total")
                                                  .orderBy(col("Total").desc()))

    revenue_distribution_by_country.show()

    print("Relationship between average unit price of products and their sales volume.")
    (orders_raw
     .join(products_raw, on="StockCode", how="inner")
     .groupBy(col("StockCode"))
     .agg({"UnitPrice": "avg", "Quantity": "sum"})
     .withColumnRenamed("avg(UnitPrice)", "AvgUnitPrice")
     .withColumnRenamed("sum(Quantity)", "TotalQuantity")
     .orderBy(col("AvgUnitPrice").desc())
     .show(10))

    print("Top 3 products with the maximum unit price drop in the last month")
    # Convert InvoiceDate to timestamp using the actual CSV format.
    # Invalid dates are converted to NULL instead of failing the whole Spark job.
    orders_with_date = (
        orders_cleansed
        .withColumn("InvoiceTimestamp", try_to_timestamp(col("InvoiceDate"), lit("M/d/yyyy H:mm")))
        .withColumn("InvoiceDate", to_date(col("InvoiceTimestamp")))
        .filter(col("InvoiceDate").isNotNull())
    )

    max_date = orders_with_date.agg(spark_max("InvoiceDate")).collect()[0][0]

    max_date_col = lit(max_date).cast("date")

    # Calculate last month's start and end dates based on the latest valid invoice date
    last_month_start = add_months(last_day(add_months(max_date_col, -2)), 1)
    last_month_end = last_day(add_months(max_date_col, -1))

    # Calculate previous month's start and end dates for comparison
    previous_month_start = add_months(last_day(add_months(max_date_col, -3)), 1)
    previous_month_end = last_day(add_months(max_date_col, -2))

    # Get products with prices from last month
    last_month_prices = (orders_with_date
                         .filter((col("InvoiceDate") >= last_month_start) & (col("InvoiceDate") <= last_month_end))
                         .join(products_cleansed, on="StockCode", how="inner")
                         .groupBy("StockCode", "Description")
                         .agg(spark_max("UnitPrice").alias("LastMonthMaxPrice")))

    # Get products with prices from previous month
    previous_month_prices = (orders_with_date
                             .filter(
        (col("InvoiceDate") >= previous_month_start) & (col("InvoiceDate") <= previous_month_end))
                             .join(products_cleansed, on="StockCode", how="inner")
                             .groupBy("StockCode")
                             .agg(spark_max("UnitPrice").alias("PreviousMonthMaxPrice")))

    # Calculate price drop and show top 3
    (last_month_prices
     .join(previous_month_prices, on="StockCode", how="inner")
     .withColumn("PriceDrop", col("PreviousMonthMaxPrice") - col("LastMonthMaxPrice"))
     .filter(col("PriceDrop") > 0)
     .orderBy(col("PriceDrop").desc())
     .select("StockCode", "Description", "PreviousMonthMaxPrice", "LastMonthMaxPrice", "PriceDrop")
     .show(3, truncate=False))

    spark.stop()


if __name__ == "__main__":
    main()
