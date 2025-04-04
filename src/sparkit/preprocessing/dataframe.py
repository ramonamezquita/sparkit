from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkit.registry import Registry
from sparkit.utils.python import unnest

registry = Registry()


@registry()
class Replace:
    """Wrapper for pyspark.sql.DataFrame.replace."""

    def __init__(
        self,
        to_replace: bool | int | float | str | list | dict,
        value: bool | int | float | str | None = None,
        subset: list | None = None,
    ):
        self.to_replace = to_replace
        self.value = value
        self.subset = subset

    def transform(self, X: DataFrame) -> DataFrame:
        return X.replace(self.to_replace, self.value, self.subset)


@registry()
class DropDuplicates:
    """Custom Transformer wrapper class for DataFrame.dropDuplicates"""

    def __init__(self, subset: list[str] | None = None):
        self.subset = subset

    def transform(self, X: DataFrame) -> DataFrame:
        return X.dropDuplicates(self.subset)


@registry()
class DropNa:
    """Custom Transformer wrapper class for DataFrame.dropna"""

    def __init__(
        self,
        how: str = "any",
        thresh: int | None = None,
        subset: str | list[str] | None = None,
    ):
        self.how = how
        self.thresh = thresh
        self.subset = subset

    def transform(self, X: DataFrame) -> DataFrame:
        return X.dropna(self.how, self.thresh, self.subset)


@registry()
class HierarchicalReplace:
    """Replace values in a column based on hierarchical conditions.

    Parameters
    ----------
    values : dict
        Nested dictionary {hier1_val: {hier2_val: {... {old_val: new_val}}}

    hierarchical_cols : list of str
        Columns defining the hierarchy [parent1, parent2,...]

    replace_col : str
        Column containing values to replace. Usually is one from
        `hierarchical_cols`.
    """

    def __init__(
        self, values: dict, hierarchical_cols: list[str], replace_col: str
    ):
        self.values = values
        self.hierarchical_cols = hierarchical_cols
        self.replace_col = replace_col

    def unnest_values(self) -> list[tuple]:
        return unnest(self.values)

    def transform(self, X: DataFrame) -> DataFrame:
        spark = X.sparkSession
        colnames = self.hierarchical_cols + ["new_value"]
        right = spark.createDataFrame(self.unnest_values(), colnames)

        merged_df = X.join(right, on=self.hierarchical_cols, how="left")

        return merged_df.withColumn(
            self.replace_col,
            F.coalesce(F.col("new_value"), F.col(self.replace_col)),
        ).drop("new_value")
