import pyspark.sql.functions as F
from pyspark import keyword_only
from pyspark.sql import DataFrame, Window
from sparkml_base_classes import TransformerBaseClass

from sparkit.typing import SQLFunction


class FillNA(TransformerBaseClass):
    """Custom Transformer wrapper class for DataFrame.fillna.

    From Docs:
    Replace null values, alias for na.fill(). DataFrame.fillna() and
    DataFrameNaFunctions.fill() are aliases of each other.

    Parameters
    ----------
    col : str or Column
        A name of the column, or the Column to drop.
    """

    @keyword_only
    def __init__(
        self,
        value: int | float | str | bool | dict = None,
        subset: str | tuple | list | None = None,
    ):
        super().__init__()

    def _transform(self, X: DataFrame) -> DataFrame:
        return X.fillna(value=self._value, subset=self._subset)


class FillMissingDates(TransformerBaseClass):
    """Adds rows corresponding to any missing dates.

    Parameters
    ----------
    group_cols : list of str
        Grouping columns.

    date_col : str of Column
        Date column.

    interval : str, default="INTERVAL 1 DAY"
        SQL Interval

    fillnull : Callable, default=F.last
        Fill values function. Must be from pyspark.sql.functions.
    """

    @keyword_only
    def __init__(
        self,
        group_cols: list[str] = None,
        date_col: str = None,
        interval: str = "INTERVAL 1 DAY",
        fillnull: SQLFunction = F.last,
    ):
        super().__init__()

    def _transform(self, X: DataFrame) -> DataFrame:

        # Create DataFrame containing all possible dates.
        all_dates_df = (
            X.groupBy(self._group_cols)
            .agg(
                F.max(self._date_col).alias("max_date"),
                F.min(self._date_col).alias("min_date"),
            )
            .select(
                *self._group_cols,
                F.sequence(
                    start="min_date",
                    stop="max_date",
                    step=F.expr(self._interval),
                ).alias(self._date_col),
            )
            .withColumn(self._date_col, F.explode(self._date_col))
        )

        # Now, left join with X and use ``fillnull`` function over a
        # window partitioned by ``group_cols`` to fill null values:
        window = Window.partitionBy(self._group_cols).orderBy(self._date_col)
        join_cols = self._group_cols + [self._date_col]
        fill_cols = [
            self._fillnull(F.col(c), ignorenulls=True).over(window).alias(c)
            for c in X.columns
            if c not in join_cols
        ]
        select_cols = join_cols + fill_cols
        X = all_dates_df.join(other=X, on=join_cols, how="left").select(
            *select_cols
        )

        return X
