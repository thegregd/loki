from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame


def store_data(df: DataFrame, output_path: str, name: str, batch_id: str | None = None) -> None:
    today: str = datetime.now().strftime("%Y-%m-%d")
    output_today_path: Path = Path(output_path) / today

    if batch_id:
        sep: str = "_"
        output_path: str = str(output_today_path / (name + sep + batch_id))
    else:
        output_path: str = str(output_today_path / name)

    df.write.mode("overwrite").parquet(output_path)
