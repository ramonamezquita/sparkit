import math

import pyspark.sql.functions as F
from pyspark import keyword_only
from pyspark.ml import Pipeline
from sparkml_base_classes import TransformerBaseClass

from .column import ColumnTransformer


class CyclicalEncoder(TransformerBaseClass):
    """Cyclical encoder.

    Encodes periodic features using sine and cosine transformations with the
    matching period.

    Parameters
    ----------
    col : str or Column
        A name of the column, or the Column to encode.

    period : int, default=10
        Input data period.

    drop : bool, default=False
        If True, drops original column.
    """

    @keyword_only
    def __init__(self, col: str = None, period: int = 1, drop: bool = False):
        super().__init__()

    def _transform(self, ddf):

        sine_transform = ColumnTransformer(
            col=self._col,
            newcol=self._col + "_sine",
            fn=lambda col: F.sin(col / self._period * 2 * math.pi),
        )

        cosine_transform = ColumnTransformer(
            col=self._col,
            newcol=self._col + "_cosine",
            fn=lambda col: F.cos(col / self._period * 2 * math.pi),
            drop=self._drop,
        )

        stages = [sine_transform, cosine_transform]
        return Pipeline(stages=stages).fit(ddf).transform(ddf)
