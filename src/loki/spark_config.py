from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    jvm_opts: str = (
        "-Djava.security.manager=allow "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "-XX:-UseContainerSupport"
    )

    spark: SparkSession = (
        SparkSession.builder
        .appName("Loki")
        .master("local[*]")
        .config("spark.driver.extraJavaOptions", jvm_opts)
        .config("spark.executor.extraJavaOptions", jvm_opts)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
