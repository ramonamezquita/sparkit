from typing import Protocol
from pyspark.sql import DataFrame


class Transformer(Protocol):
    """Protocol for data transformers that apply a transformation to a 
    PySpark DataFrame.

    Transformers must apply :meth:`transform` with signature:
                `transform(X: DataFrame) -> DataFrame`
    """

    def transform(X: DataFrame) -> DataFrame: ...
