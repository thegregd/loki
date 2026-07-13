from pyspark.sql import DataFrame, SparkSession

from loki.schemas import customers_schema, orders_schema, products_schema


def read_customers_data(customers_input_path: str, spark: SparkSession) -> DataFrame:
    customers_input_df = (spark.read
                          .schema(customers_schema)
                          .option("header", True)
                          .option("emptyValue", "")
                          .csv(customers_input_path))

    return customers_input_df


def read_orders_data(orders_input_path: str, spark: SparkSession) -> DataFrame:
    orders_input_df = (spark.read
                       .schema(orders_schema)
                       .option("header", True)
                       .option("dateFormat", "MM/dd/yyyy HH:mm")
                       .option("emptyValue", "")
                       .csv(orders_input_path))

    return orders_input_df


def read_products_data(products_input_path: str, spark: SparkSession) -> DataFrame:
    products_input_df = (spark.read
                         .schema(products_schema)
                         .option("header", True)
                         .option("emptyValue", "")
                         .csv(products_input_path))

    return products_input_df
