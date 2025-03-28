from functools import reduce
from logging import Logger
from typing import Callable

from pyspark.sql import DataFrame

from .transformer import Transformer


def make_pipeline(
    transformers: list[Transformer], logger: Logger | None = None
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

    def apply_transform(x: DataFrame, transformer: Transformer) -> DataFrame:
        if logger:
            logger.info(
                f"Applying transformer `{transformer.__class__.__name__}`"
            )
        return transformer.transform(x)

    def pipeline(X) -> DataFrame:
        return reduce(apply_transform, transformers, X)

    return pipeline
