from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from sparkit.registry import Registry


def _check_pyspark_function(name: str) -> None:
    if not hasattr(F, name):
        raise ValueError(
            f"PySpark function `{name}` does not exist in `pyspark.sql.functions`."
        )


def _check_column_method(name: str):
    if not hasattr(Column, name):
        raise ValueError(f"PySpark Column method `{name}` does not exist.")


def _check_dataframe_method(name: str) -> None:
    if not hasattr(DataFrame, name):
        raise ValueError(f"PySpark DataFrame method `{name}` does not exist.")


registry = Registry()


@registry()
class ColumnTransformer:
    """Applies a method from the PySpark Column API to multiple DataFrame columns.

    For each column in `input_cols`, the transformation is applied as
    `col.<method>(*args, **kwargs)` using PySpark's Column object methods
    (e.g., "cast", "alias", "isNotNull").

    Parameters
    ----------
    name : str
        Name of the method to apply from the PySpark Column API.

    cols : list of str
        List of columns to transform.

    args : list, optional
        Positional arguments to pass to the method.

    kwargs : dict, optional
        Keyword arguments to pass to the method.
    """

    def __init__(
        self,
        name: str,
        cols: list[str],
        args: list = None,
        kwargs: dict = None,
    ):

        _check_column_method(name)
        self.name = name
        self.cols = cols
        self.args = args or []
        self.kwargs = kwargs or {}

    def transform(self, X: DataFrame) -> DataFrame:
        # fmt: off
        select_cols = [
            getattr(F.col(c), self.name)(*self.args, **self.kwargs).alias(c) 
            if c in self.cols
            else F.col(c) 
            for c in X.columns
        ]
        # fmt: on

        return X.select(*select_cols)


@registry()
class FunctionTransformer:
    """Applies a PySpark SQL function to multiple DataFrame columns.

    For each col in `input_cols`, the function is applied as
    `fun(col, *args, **kwargs)`.

    Parameters
    ----------
    name : str
        Name of the function from pyspark.sql.functions (e.g., "lower", "trim",
        "regexp_replace").

    cols : list of str
        List of columns to transform.

    args : list, optional
        Additional arguments to pass to the function, after the column.

    kwargs : dict, optional
        Keyword arguments for the function.
    """

    def __init__(
        self,
        name: str,
        cols: list[str],
        args: list = None,
        kwargs: dict = None,
    ):

        _check_pyspark_function(name)
        self.name = name
        self.cols = cols
        self.args = args or []
        self.kwargs = kwargs or {}

    def transform(self, X: DataFrame) -> DataFrame:
        fn = getattr(F, self.name)

        # fmt: off
        select_cols = [
            fn(F.col(c), *self.args, **self.kwargs).alias(c)
            if c in self.cols
            else F.col(c)
            for c in X.columns
        ]
        # fmt: on

        return X.select(*select_cols)


@registry()
class DataFrameTransformer:
    """Applies a method from the PySpark DataFrame API to the entire DataFrame.

    The specified method is applied as
    `df.<method>(*args, **kwargs)`, allowing for transformations such as
    `dropna`, `fillna`, `dropDuplicates`, etc.

    Parameters
    ----------
    name : str
        Name of the method to apply from the PySpark DataFrame API.

    args : list, optional
        Positional arguments to pass to the method.

    kwargs : dict, optional
        Keyword arguments to pass to the method.
    """

    def __init__(
        self,
        name: str,
        args: list = None,
        kwargs: dict = None,
    ):
        _check_dataframe_method(name)
        self.name = name
        self.args = args or []
        self.kwargs = kwargs or {}

    def transform(self, X: DataFrame) -> DataFrame:
        method = getattr(X, self.name)
        return method(*self.args, **self.kwargs)
