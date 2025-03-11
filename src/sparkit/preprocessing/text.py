from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from thefuzz import process


def _ascii_ignore(x: str) -> str:
    return x.encode("ascii", "ignore").decode("ascii")


class TextStandardizer:
    def __init__(
        self, col: str, standard_values: list[str], threshold: int = 85
    ):
        self.col = col
        self.standard_values = standard_values
        self.threshold = threshold

    def transform(self, X: DataFrame) -> DataFrame:

        def best_match(value: str) -> str:
            """Find closest value using fuzzy matching"""
            match, score = process.extractOne(value, self.standard_values)
            return match if score > self.threshold else value

        best_match_udf = udf(best_match)

        return X.withColumn(self.col, best_match_udf(F.col(self.col)))


class TextNormalizer:
    def __init__(self, cols: list[str], initcap: bool = False):
        self.initcap = initcap
        self.cols = cols

    def transform(self, X: DataFrame) -> DataFrame:

        ascii_udf = udf(_ascii_ignore)
        select_cols = []

        for c in X.columns:

            if c in self.cols:
                c = (
                    ascii_udf(F.initcap(F.col(c))).alias(c)
                    if self.initcap
                    else ascii_udf(F.col(c)).alias(c)
                )
            else:
                c = F.col(c)

            select_cols.append(c)

        return X.select(*select_cols)
