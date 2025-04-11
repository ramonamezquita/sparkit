from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from sparkit.utils.python import unnest


def apply_dataframe_method(
    X: DataFrame,
    method_name: str,
    method_args: tuple = (),
    method_kwargs: dict | None = None,
) -> DataFrame:

    method_kwargs = method_kwargs or {}
    method = getattr(X, method_name)

    if method is None:
        raise ValueError(
            f"PySpark DataFrame method `{method_name}` does not exist."
        )

    return method(*method_args, **method_kwargs)


def apply_column_method(
    col: Column,
    method_name: str,
    method_args: tuple = (),
    method_kwargs: dict | None = None,
) -> Column:
    method_kwargs = method_kwargs or {}
    method = getattr(col, method_name)

    if not hasattr(Column, method_name):
        raise ValueError(
            f"PySpark Column method `{method_name}` does not exist."
        )

    return method(*method_args, **method_kwargs)


def hierarchical_replace(
    X: DataFrame,
    values: dict,
    hierarchical_cols: list[str],
    replace_col: str,
) -> DataFrame:
    spark = X.sparkSession
    colnames = hierarchical_cols + ["new_value"]
    right = spark.createDataFrame(unnest(values), colnames)
    merged_df = X.join(right, on=hierarchical_cols, how="left")

    return merged_df.withColumn(
        replace_col,
        F.coalesce(F.col("new_value"), F.col(replace_col)),
    ).drop("new_value")
