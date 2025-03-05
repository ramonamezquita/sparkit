from functools import reduce
from typing import Callable

from pyspark.sql import DataFrame

from .transformer import Transformer


def make_pipeline(
    transformers: list[Transformer],
) -> Callable[[DataFrame], DataFrame]:
    """Creates a pipeline that sequentially applies a list of Transformers to a
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

    def pipeline(X) -> DataFrame:
        return reduce(
            lambda x, transformer: transformer.transform(x), transformers, X
        )

    return pipeline
