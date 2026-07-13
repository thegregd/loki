import os
from collections.abc import Generator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession, DataFrame

from loki.data_writer import store_data
from loki.spark_config import get_spark_session


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    spark: SparkSession = get_spark_session()
    yield spark
    spark.stop()


def test_write_raw_data_with_batch_id(tmp_path: Path, spark: SparkSession) -> None:
    # given
    path: Path = tmp_path / "data"

    name: str = "test_name"
    batch_id: str = "123"
    df: DataFrame = spark.createDataFrame([("John", 25), ("Jane", 30)], ["name", "age"])
    output_path = Path(path)

    tmp_path.parts[0]

    # when
    store_data(df, output_path, name, batch_id)
    actual_dirs = []
    for _, dirs, files in os.walk(tmp_path):
        actual_dirs.extend(dirs)

    # then
    assert "test_name_123" in actual_dirs, "Should have batch id as part of the name"


def test_write_raw_data_no_batch_id(tmp_path: Path, spark: SparkSession) -> None:
    # given
    path: Path = tmp_path / "data"

    name: str = "test_name"
    df: DataFrame = spark.createDataFrame([("John", 25), ("Jane", 30)], ["name", "age"])
    output_path = Path(path)

    tmp_path.parts[0]

    # when
    store_data(df, output_path, name)
    actual_dirs = []
    for _, dirs, files in os.walk(tmp_path):
        actual_dirs.extend(dirs)

    # then
    assert "test_name" in actual_dirs, "Should not have batch id as part of the name"
