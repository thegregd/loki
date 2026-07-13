from pyspark.sql.types import StructType, StructField, StringType, DecimalType, LongType

customers_schema: StructType = StructType([
    StructField("CustomerID", DecimalType(), True),
    StructField("Country", StringType(), True),
])

orders_schema: StructType = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Quantity", LongType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("CustomerID", DecimalType(), True),
])

products_schema: StructType = StructType([
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("UnitPrice", DecimalType(scale=4), True),
])
