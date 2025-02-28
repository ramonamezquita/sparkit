import pyspark.sql.functions as F
from pyspark import keyword_only
from pyspark.sql import DataFrame
from sparkml_base_classes import TransformerBaseClass

from sparkit.registry import Registry
from sparkit.typing import ColumnOrName, DatetimeAttribute, SQLFunction

from .column import MultiColumnTransformer

registry = Registry(prefix="date_")


def _get_datetime_func(name: DatetimeAttribute) -> SQLFunction:
    """Returns datetime function from module pyspark.sql.function.

    These functions extract datetime attributes from a given date/timestamp.

    Parameters
    ----------
    name : DatetimeAttribute
        Name of the datetime attr. Example: "dayofweek".
    """

    name_to_fn: dict[str, SQLFunction] = {
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
class DateNormalizer(TransformerBaseClass):
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

    @keyword_only
    def __init__(self, col=None, format_=None):
        super().__init__()

    def _transform(self, ddf):
        date_col = F.to_date(F.col(self._col), self._format_)
        return ddf.withColumn(self._col, date_col)


class DatetimeExtractor(TransformerBaseClass):
    """Extracts datetime attributes (e.g. "dayofweek") from date column.

    Parameters
    ----------
    date_col : str or Column
        Date column.

    attrs : tuple of DatetimeAttribute.
        Datetime attributes to extract.
    """

    @keyword_only
    def __init__(
        self,
        date_col: ColumnOrName = None,
        attrs: tuple[DatetimeAttribute] = None,
    ):
        super().__init__()

    def _transform(self, X: DataFrame) -> DataFrame:

        fns: list[SQLFunction] = [
            _get_datetime_func(attr) for attr in self._attrs
        ]

        return MultiColumnTransformer(
            col=self._date_col, fns=fns, new_cols=self._attrs
        ).transform(X)
