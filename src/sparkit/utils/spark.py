import os

from pyspark import SparkFiles
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField


def count_nan(data: DataFrame, subset: list[str] | None = None) -> DataFrame:
    """Counts NaN or null values in specified columns of a DataFrame.

    Parameters
    ----------
    data : DataFrame
        The input Spark DataFrame.

    subset : list[str] | None
        List of column names to check for NaNs. If None, all columns are checked.

    Returns
    -------
    DataFrame
        A single-row DataFrame with counts of NaNs/nulls per column.
    """
    if subset is None:
        subset = data.columns

    return data.select(
        [
            F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c)
            for c in subset
        ]
    )


def assign_ids(data: DataFrame, id_col: str, start_id: int = 0) -> DataFrame:
    """Assigns unique IDs to each row in a DataFrame, starting from a given value.

    Parameters
    ----------
    data : DataFrame
        The input Spark DataFrame.

    id_col : str
        Name of the column to store generated IDs.

    start_id : int
        Starting value for ID assignment.

    Returns
    -------
    DataFrame
        DataFrame with an additional column containing unique IDs.
    """
    return data.withColumn(
        id_col, F.monotonically_increasing_id() + F.lit(start_id)
    )


def deduplicate(data: DataFrame, columns: list[str]) -> DataFrame:
    """Removes duplicate rows based on a subset of columns.

    Parameters
    ----------
    data : DataFrame
        The input Spark DataFrame.

    columns : list[str]
        List of column names used to identify duplicates.

    Returns
    -------
    DataFrame
        DataFrame containing only distinct rows based on the specified columns.
    """
    return data.select(*columns).distinct()


def union(data: DataFrame, other: DataFrame) -> DataFrame:
    """Union between two dataframes."""
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
    """
    return (data.count(), len(data.columns))


def get_spark_filepath(file_name: str, check_exists: bool = True) -> str:
    """Retrieves path for a file that was added to Spark's file system.

    Parameters
    ----------
    file_name : str
        Name of the file to locate in Spark's distributed cache.

    check_exists : bool, optional
        If True (default), verifies that the file exists in the local filesystem.
        If False, returns the path without checking file existence.

    Returns
    -------
    filepath: str
        Absolute path to the file in the local filesystem where Spark has 
        cached it.
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
