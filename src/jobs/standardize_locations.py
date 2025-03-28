from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField
from pyspark.sql.window import Window
from tinytask.callbacks import LoggingCallback
from tinytask.decorators import task

from sparkit import objects as O
from sparkit.logging import create_default_logger
from sparkit.tasks import etl
from sparkit.utils import check_fields, check_keys

JOB_NAME = "StardadizeLocationsETL"


__logger__ = create_default_logger(name=JOB_NAME)


def _get_admin1_map(input_tb: DataFrame, standard_tb: DataFrame) -> DataFrame:
    """Creates a mapping between input names and standard names using
    Levenshtein distance for best matching.

    Returns
    -------
    DataFrame
        A mapping DataFrame with input admin1 names and their closest
        standardized match.
    """
    admin1_filter = F.col("featurecode") == "ADM1"
    admin1_tb = standard_tb.filter(admin1_filter).select("name", "admin1code")
    admin1_tb = admin1_tb.alias("admin1_tb")

    lev_col = F.levenshtein("admin1_tb.name", "input_tb.admin1name")
    admin1_lev_tb = (
        admin1_tb.select("name")
        .crossJoin(input_tb.select("admin1name").distinct().alias("input_tb"))
        .withColumn("lev", lev_col)
    )

    window = Window.partitionBy("admin1name").orderBy("lev")

    admin1_map = (
        admin1_lev_tb.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .select("input_tb.admin1name", "admin1_tb.name")
    )

    return admin1_map


@task(name="Extract", callbacks=[LoggingCallback(__logger__)])
def extract(sources: list[etl.Source]) -> O.ObjectStore:
    """Extracts data from multiple sources."""
    spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()

    os = O.ObjectStore()
    extraction = etl.extract_many(sources, spark)
    os.add(key="extraction", value=extraction)
    return os


@task()
def transform(os: O.ObjectStore) -> DataFrame:

    # Check extraction
    extraction: dict[str, DataFrame] = os.get("extraction")
    mandatory_keys = ["input_tb", "standard_tb"]
    check_keys(extraction, mandatory_keys, "extraction")
    input_tb = extraction["input_tb"]
    standard_tb = extraction["standard_tb"]

    # Check input fields
    fields = [
        StructField("admin1name", StringType()),
        StructField("admin2name", StringType()),
    ]
    check_fields(input_tb, fields)

    # ADM1 Standardization
    admin1_map = _get_admin1_map(input_tb, standard_tb)

    # ADM2 Standardization


@task()
def load(os, output_uri: str) -> DataFrame:
    pass


def run(
    input_uri: str,
    standard_uri: str,
    output_uri: str = "output.parquet",
):
    """Standardizes locations by performing fuzzy matching."""
    pass
