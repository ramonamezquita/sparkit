from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from sparkit.registry import Registry

registry = Registry()


@registry()
class RegexReplace:
    """Custom Transformer wrapper class for F.regexp_replace

    Applies regex replacement to specified columns in a DataFrame.

    Parameters
    ----------
    cols: list of str
        List of column names to apply the regex replacement

    pattern: str
        Regex pattern to match (as string)

    replacement: str
        Replacement string for matched patterns
    """

    def __init__(
        self,
        cols: list[str],
        pattern: str,
        replacement: str,
    ):
        self.cols = cols
        self.pattern = pattern
        self.replacement = replacement

    def transform(self, X: DataFrame) -> DataFrame:

        cols = [
            (
                F.regexp_replace(col, self.pattern, self.replacement).alias(col)
                if col in self.cols
                else X[col]
            )
            for col in X.columns
        ]

        return X.select(*cols)


@registry()
class Cast:
    """Cast multiple columns into given types.

    Parameters
    ----------
    dtypes : dict str -> dtype
        Column name to dtype.
    """

    def __init__(self, dtypes: dict):
        self.dtypes = dtypes

    def transform(self, X: DataFrame) -> DataFrame:
        all_dtypes = {col: dtype for col, dtype in X.dtypes}
        all_dtypes.update(self.dtypes)
        cols = [F.col(col).cast(dtype) for col, dtype in all_dtypes.items()]
        return X.select(*cols)


@registry()
class Rename:
    """Custom Transformer wrapper class for DataFrame.withColumnsRenamed.

    From Docs:
    Returns a new DataFrame by renaming multiple columns.
    This is a no-op if the schema doesn't contain the given column names.

    Parameters
    ----------
    cols_map : dict
        A dict of existing column names and corresponding desired column names.
    """

    def __init__(self, cols_map: dict):
        self.cols_map = cols_map

    def transform(self, X: DataFrame) -> DataFrame:
        return X.withColumnsRenamed(self.cols_map)


@registry()
class Drop:
    """Custom Transformer wrapper class for DataFrame.drop.

    From Docs:
    Returns a new DataFrame without specified columns.
    This is a no-op if the schema doesn't contain the given column name(s).

    Parameters
    ----------
    col : str or Column
        A name of the column, or the Column to drop.
    """

    def __init__(self, col: str):
        self.col = col

    def transform(self, X: DataFrame) -> DataFrame:
        return X.drop(self.col)


@registry()
class ColumnTransformer:
    """Constructs a transformer from a pyspark sql function.

    A ColumnTransformer forwards the column object from its input dataframe
    to a function object and returns the result of this function.

    Parameters
    ----------
    col : str or Column
        A name of the column, or the Column to transform.

    new_col : str or Column
        A name for the new transformed column. Choose this to be the same as
        ``col`` to overwrite its values.

    fn : SQLCallable
        Callable from pyspark.sql.functions

    kwargs : dict, default=None
        Kwargs to propagate to `fn`.
    """

    def __init__(self, col: str, new_col: str, fn, kwargs: dict):

        self.col = col
        self.new_col = new_col
        self.fn = fn
        self.kwargs = kwargs

    def transform(self, X: DataFrame) -> DataFrame:
        kwargs = {} if self.kwargs is None else self.kwargs
        Xt = X.withColumn(self.new_col, self.fn(F.col(self.col), **kwargs))
        return Xt


@registry()
class Select:
    """Custom Transformer wrapper class for DataFrame.select.

    Parameters
    ----------
    cols : list of str or Column
        Column names (string) or expressions (Column). If one of the column
        names is '*', that column is expanded to include all columns in the
        current DataFrame.
    """

    def __init__(self, cols: list[str] = None):
        self.cols = cols

    def transform(self, X: DataFrame) -> DataFrame:
        return X.select(*self.cols)


class MultiColumnTransformer:
    """Applies multiple transformations to a single column.

    Each transformation corresponds to a new column in the returned DataFrame.

    Notes
    -----
    This transformers introduces multiple projections internally. Therefore,
    calling it with multiples functions to add multiple columns can generate
    big plans which can cause performance issues and even
    StackOverflowException. To avoid this, use :class:`ColumnSelector` with the
    multiple columns at once.

    """

    def __init__(
        self,
        col: str,
        fns: list[Callable],
        new_cols: list[str],
    ):
        self.col = col
        self.fns = fns
        self.new_cols = new_cols

    def transform(self, X: DataFrame) -> DataFrame:

        for fn, new_col in zip(self.fns, self.new_cols):
            ct = ColumnTransformer(fn=fn, col=self.col, new_col=new_col)
            X = ct.transform(X)

        return X
