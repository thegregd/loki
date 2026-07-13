from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import count


def cleanse_customers_data(customers_raw: DataFrame) -> DataFrame:
    customers_no_null: DataFrame = customers_raw.dropna()
    customers_duplicates: DataFrame = (customers_no_null
                                       .withColumn("duplicated",
                                                   count("*").over(Window.partitionBy(["CustomerID", "Country"])) > 1))

    print("Customers duplicates on [CustomerID, Country]")
    customers_duplicates.filter(customers_duplicates.duplicated).show(truncate=False)
    customers_cleansed: DataFrame = (customers_no_null
                                     .dropDuplicates(["CustomerID", "Country"])
                                     .drop("duplicated"))

    return customers_cleansed


def cleanse_orders_data(orders_raw: DataFrame) -> DataFrame:
    orders_no_null: DataFrame = orders_raw.dropna()
    orders_duplicates: DataFrame = (orders_no_null
                                    .withColumn("duplicated",
                                                count("*").over(Window.partitionBy(["InvoiceNo", "StockCode"])) > 1))

    print("Orders duplicates on [InvoiceNo, StockCode]")
    orders_duplicates.filter("duplicated").show(truncate=False)
    orders_cleansed: DataFrame = (orders_no_null
                                  .dropDuplicates()
                                  .drop("duplicated"))

    return orders_cleansed


def cleanse_products_data(products_raw: DataFrame) -> DataFrame:
    products_no_null: DataFrame = products_raw.dropna()

    products_no_null_duplicates: DataFrame = (products_no_null
                                              .withColumn("duplicated",
                                                          count("*").over(Window.partitionBy(["StockCode"])) > 1))

    print("Products duplicates on [StockCode]")
    products_no_null_duplicates.filter("duplicated").show(truncate=False)

    print("Products with duplicates, should be empty")
    products_no_duplicates_check: DataFrame = (products_no_null
                                               .withColumn("duplicated",
                                                           count("*").over(Window.partitionBy(["StockCode"])) > 1))
    products_no_duplicates_check.filter("duplicated").show(truncate=False)

    print("Products without duplicates")
    products_no_null.show(truncate=False)
    products_cleansed: DataFrame = products_no_null.drop("duplicated")

    return products_cleansed
