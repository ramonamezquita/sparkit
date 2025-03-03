from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from sparkit.registry import Registry

from .column import MultiColumnTransformer

registry = Registry(prefix="date_")


def _get_datetime_func(name: str) -> Callable:
    """Returns datetime function from module pyspark.sql.function.

    These functions extract datetime attributes from a given date/timestamp.

    Parameters
    ----------
    name : DatetimeAttribute
        Name of the datetime attr. Example: "dayofweek".
    """

    name_to_fn: dict[str, Callable] = {
        "dayofweek": F.dayofweek,
        "month": F.month,
    }

    if name not in name_to_fn:
        raise ValueError(
            f"Date attribute '{name}' is not valid. "
            f"Available options are: {list(name_to_fn)}."
        )

    return name_to_fn[name]


@registry(name="normalizer")
class DateNormalizer:
    """Custom Transformer wrapper class for functions.date_format().

    From Docs:
    Converts a date/timestamp/string to a value of string in the format
    specified by the date format given by the second argument.

    Parameters
    ----------
    col : str
        Column name to cast to date type.

    format : str
        The strftime to parse time, e.g. "%d/%m/%Y".
    """

    def __init__(self, col: str, format: str):
        self.col = col
        self.format = format

    def transform(self, X: DataFrame) -> DataFrame:
        date_col = F.to_date(F.col(self.col), self.format)
        return X.withColumn(self.col, date_col)


class DatetimeExtractor:
    """Extracts datetime attributes (e.g. "dayofweek") from date column.

    Parameters
    ----------
    date_col : str or Column
        Date column.

    attrs : tuple of DatetimeAttribute.
        Datetime attributes to extract.
    """

    def __init__(
        self,
        date_col: str,
        attrs: tuple[str],
    ):
        self.date_col = date_col
        self.attrs = attrs

    def transform(self, X: DataFrame) -> DataFrame:

        fns = [_get_datetime_func(attr) for attr in self.attrs]

        return MultiColumnTransformer(
            col=self.date_col, fns=fns, new_cols=self.attrs
        ).transform(X)
