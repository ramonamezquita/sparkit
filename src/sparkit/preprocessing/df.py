from pyspark.sql import DataFrame

from sparkit.registry import Registry

registry = Registry("df_")


@registry("replace")
class DataFrameReplace:
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
