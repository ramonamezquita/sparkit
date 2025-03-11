from typing import Protocol

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.readwriter import DataFrameReader

from sparkit.factory import create_factory
from sparkit.registry import Registry

registry = Registry()


class Extractor(Protocol):
    """Extractor interface.


    Extracts are objects implementing the :class:`Extractor` interface.
    That is, method :meth:`extract` must be implemented. For example,
    the following class is a convenience wrapper around pandas to allows
    to satisfy the :class:`Extractor` interface:

    ```
    import pandas as pd

    class PandasSQLExtractor:
        def __init__(self, sql, con):
            self.sql=sql
            self.con=con

        def extract(self) -> DataFrame
            return pd.read_sql(sql=sql, con=con)
    ```
    """

    def extract(self) -> DataFrame:
        """Extracts data from arbitrary data source."""
        ...


class SparkExtractor:

    def __init__(
        self,
        reader: DataFrameReader,
        filepath: str,
        options: dict | None = None,
    ) -> None:
        self.reader = reader
        self.filepath = filepath
        self.options = options

    def extract(self) -> DataFrame:
        options = self.options or {}
        return self.reader(self.filepath, **options)


@registry("csv")
class SparkCSVExtractor(SparkExtractor):
    """Wrapper for pyspark csv reader.
    """
    def __init__(
        self, spark: SparkSession, filepath, options: dict | None = None
    ):
        super().__init__(spark.read.csv, filepath, options)


@registry("parquet")
class SparkParquetExtractor(SparkExtractor):
    """Wrapper for pyspark parquet reader.
    """
    def __init__(
        self, spark: SparkSession, filepath: str, options: dict | None = None
    ):
        super().__init__(spark.read.parquet, filepath, options)


#: Extractors factory.
factory = create_factory([registry])
