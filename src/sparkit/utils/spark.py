import os

from pyspark import SparkFiles
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField


def assign_ids(data: DataFrame, id_col: str, start_id: int = 0) -> DataFrame:
    return data.withColumn(
        id_col, F.monotonically_increasing_id() + F.lit(start_id)
    )


def deduplicate(data: DataFrame, columns: list[str]) -> DataFrame:
    return data.select(*columns).distinct()


def union(data: DataFrame, other: DataFrame) -> DataFrame:
    return data.unionByName(other)


def shape(data: DataFrame) -> tuple[int, int]:
    """Returns the dimensions of a Spark DataFrame as a tuple.

    This function mimics the behavior of pandas DataFrame.shape property.

    Parameters
    ----------
    data : pyspark.sql.DataFrame
        The Spark DataFrame to get dimensions for.

    Returns
    -------
    tuple[int, int]
        A tuple containing two integers:
        - First element: Number of rows in the DataFrame (obtained using df.count())
        - Second element: Number of columns in the DataFrame (obtained using len(df.columns))

    Examples
    --------
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> data = [("Alice", 25), ("Bob", 30)]
    >>> df = spark.createDataFrame(data, ["name", "age"])
    >>> shape(df)
    (2, 2)

    Notes
    -----
    - The row count is computed using df.count(), which triggers an action and may be
      computationally expensive for large DataFrames.
    - Unlike pandas' shape property, this is implemented as a function.
    """
    return (data.count(), len(data.columns))


def get_spark_filepath(file_name: str, check_exists: bool = True) -> str:
    """Retrieves the local filesystem path for a file that was added to
        Spark's file distribution system.

        Parameters
        ----------
        file_name : str
            Name of the file to locate in Spark's distributed cache.

        check_exists : bool, optional
            If True (default), verifies that the file exists in the local filesystem.
            If False, returns the path without checking file existence.
    f
        Returns
        -------
        filepath: str
            Absolute path to the file in the local filesystem where Spark has cached it.


        Examples
        --------
        >>> # Assuming 'data.csv' was included in spark-submit --files
        >>> filepath = get_spark_filepath('data.csv')
        >>> with open(filepath, 'r') as f:
        ...     data = f.read()
    """

    filepath = SparkFiles.get(file_name)

    if check_exists:
        exists = os.path.exists(filepath)
        if not exists:
            raise ValueError(
                "File {file_name} does not exists in spark files directory. "
                "Upload the corresponding file using the `--files` option in "
                "the `spark-submit` command."
            )

    return filepath


def check_fields(df: DataFrame, fields: list[StructField]) -> None:
    """Checks that all expected StructFields are present in the DataFrame schema.

    Args:
        df (DataFrame): The Spark DataFrame to check.
        fields (list[StructField]): A list of expected StructField objects.

    Raises:
        ValueError: If any expected field is missing or incorrect.
    """
    for f in fields:
        if f not in df.schema:
            raise ValueError(
                f"Schema mismatch: {f} is missing or incorrect in DataFrame schema."
            )
