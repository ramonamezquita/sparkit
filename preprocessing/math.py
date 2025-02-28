import pyspark.sql.functions as F
from pyspark import keyword_only
from pyspark.sql import DataFrame
from sparkml_base_classes import TransformerBaseClass


class TryDivide(TransformerBaseClass):
    """Custom Transformer wrapper class for functions.try_divide().

    Parameters
    ----------
    left: str
        Dividend.

    right: str
        Divisor.

    newcol : str or Column
        A name for the new column, or a Column instance.
    """

    @keyword_only
    def __init__(self, left: str = None, right: str = None, newcol: str = None):
        super().__init__()

    def _transform(self, X: DataFrame) -> DataFrame:
        return X.withColumn(self._newcol, F.try_divide(self._left, self._right))
