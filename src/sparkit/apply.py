from abc import ABC
from functools import reduce
from types import SimpleNamespace
from typing import Callable, Literal, TypedDict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkit.factory import create_factory
from sparkit.registry import Registry
from sparkit.sql import functions as S

_NAME_TO_MODULE = {"pyspark": F, "sparkit": S}


def _get_module(name: str) -> SimpleNamespace:
    try:
        return _NAME_TO_MODULE[name]
    except KeyError:
        raise KeyError(f"Unknown functional module `{name}`.")


def _check_module(attr, module_name) -> Callable:
    module = _get_module(module_name)
    attr = getattr(module, attr)
    if attr is None:
        raise ValueError(
            f"Module functional `{module_name}` does not have attribute `{attr}`."
        )

    return attr


registry = Registry()


class SerializedApplier(TypedDict):
    """Represents an Applier object"""

    on: str
    func: str
    args: tuple
    kwargs: dict[str, str]
    cols: list[str]
    module: str


class Applier(ABC):

    def __init__(
        self,
        func: Callable,
        args: tuple = (),
        kwargs: dict | None = None,
        cols: list[str] | None = None,
        module: Literal["pyspark", "sparkit"] = "pyspark",
    ):
        self.func = func
        self.cols = cols
        self.args = args or []
        self.kwargs = kwargs or {}
        self.module = module
        self._fn = _check_module(func, module)

    def apply(X: DataFrame) -> DataFrame:
        pass


@registry("columns")
class Column(Applier):
    """Applies a function to multiple columns.

    For each column in `cols`, the function is applied as
    `func(*args, **kwargs)`.

    Parameters
    ----------
    func : str
        Name of the function from pyspark.sql.functions (e.g., "lower", "trim",
        "regexp_replace") or sparkit.sql.functions.

    cols : list of str
        List of columns to transform.

    args : list, optional
        Positional arguments to pass to the function.

    kwargs : dict, optional
        Keyword arguments to pass to the function.

    module : str {"psypark", "sparkit}, default="pyspark"
        Which functional (functions module) to use.
    """

    def apply(self, X: DataFrame) -> DataFrame:

        # fmt: off
        select_cols = [
            self._fn(F.col(c), *self.args, **self.kwargs).alias(c) 
            if c in self.cols
            else F.col(c) 
            for c in X.columns
        ]
        # fmt: on

        return X.select(*select_cols)


@registry("dataframe")
class DataFrame(Applier):
    """Applies a function to DataFrame.

    Parameters
    ----------
    func : str
        Name of the function from pyspark.sql.functions (e.g., "lower", "trim",
        "regexp_replace") or sparkit.sql.functions.

    cols : list of str
        List of columns to transform.

    args : list, optional
        Additional arguments to pass to the function, after the column.

    kwargs : dict, optional
        Keyword arguments for the function.

    module : str {"psypark", "sparkit}, default="pyspark"
        Which functional (functions module) to use.
    """

    def apply(self, X: DataFrame) -> DataFrame:
        subset = self.cols or X.columns
        return self._fn(X[subset], *self.args, **self.kwargs)


factory = create_factory([registry])


def to_factory_args(serialized_applier: SerializedApplier) -> dict:
    copy = serialized_applier.copy()
    return {"name": copy.pop("on"), "args": copy}


def make_apply_chain(
    appliers: list[Applier],
) -> Callable[[DataFrame], DataFrame]:
    """
    Creates a pipeline that sequentially applies a list of Transformers to a
    PySpark DataFrame.

    Parameters
    ----------
    transformers : list[Transformer]
        A list of Transformer instances to apply in order.

    Returns
    -------
    Callable[[DataFrame], DataFrame]
        A function that takes a DataFrame and applies the transformations
        sequentially.
    """

    def apply(x: DataFrame, applier: Applier) -> DataFrame:
        return applier.apply(x)

    def chain(X) -> DataFrame:
        return reduce(apply, appliers, X)

    return chain
