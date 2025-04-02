from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField
from pyspark.sql.window import Window
from tinytask.callbacks import LoggingCallback
from tinytask.configfile import ConfigFile
from tinytask.decorators import task

from sparkit import objects as O
from sparkit.logging import create_default_logger
from sparkit.tasks import etl
from sparkit.utils.python import check_keys
from sparkit.utils.spark import check_fields

JOB_NAME = "MatchGeoNamesETL"


__logger__ = create_default_logger(name=JOB_NAME)


def _create_admin1_map(
    input_tb: DataFrame, standard_tb: DataFrame
) -> DataFrame:
    """Creates a mapping between admin1 input names and admin1 standard names.

    Returns
    -------
    DataFrame
        A mapping DataFrame with input admin1 names and their closest
        standardized match.
    """
    window = Window.partitionBy("admin1name").orderBy("levenshtein")

    admin1_map = (
        standard_tb.filter(F.col("featurecode") == "ADM1")
        .alias("standard_tb")
        .crossJoin(input_tb.select("admin1name").distinct().alias("input_tb"))
        .withColumn(
            "levenshtein",
            F.levenshtein("standard_tb.name", "input_tb.admin1name"),
        )
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .select(
            "input_tb.admin1name",
            F.col("standard_tb.name").alias("stdname"),
        )
    )

    return admin1_map


def _create_admin2_map(
    standard_tb: DataFrame, input_tb: DataFrame
) -> DataFrame:
    """Creates a mapping between admin2 input names and admin2 standard names.

    Returns
    -------
    DataFrame
        A mapping DataFrame with input admin2 names and their closest
        standardized match.
    """
    admin1_name_to_code_tb = standard_tb.filter(
        F.col("featurecode") == "ADM1"
    ).select(F.col("name").alias("admin1name"), "admin1code")

    window = Window.partitionBy("admin1name", "admin2name").orderBy(
        "levenshtein"
    )

    admin2_map = (
        standard_tb.filter(F.col("featurecode") == "ADM2")
        .join(admin1_name_to_code_tb, "admin1code")
        .alias("standard_tb")
        .join(
            input_tb.select("admin1name", "admin2name")
            .distinct()
            .alias("input_tb"),
            "admin1name",
        )
        .withColumn(
            "levenshtein",
            F.levenshtein("standard_tb.name", "input_tb.admin2name"),
        )
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank", "levenshtein")
        .select(
            "input_tb.admin1name",
            "input_tb.admin2name",
            F.col("standard_tb.name").alias("stdname"),
            "longitude",
            "latitude",
            "timezone",
        )
    )

    return admin2_map


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
    admin1_map = _create_admin1_map(input_tb, standard_tb)
    output_tb = (
        input_tb.join(admin1_map, "admin1name")
        .drop("admin1name")
        .withColumnRenamed("stdname", "admin1name")
    )

    # ADM2 Standardization
    admin2_map = _create_admin2_map(standard_tb, output_tb)
    if admin2_map.isEmpty():
        raise ValueError("Admin2 mapping resulted in an empty table.")

    output_tb = (
        output_tb.join(admin2_map, ["admin1name", "admin2name"])
        .drop("admin2name")
        .withColumnRenamed("stdname", "admin2name")
    )

    return output_tb


@task(name="Load", callbacks=[LoggingCallback(__logger__)])
def load(to_load: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Loads validated data into target destinations.

    Parameters
    ----------
    to_load : DataFrame
        DataFrame to load.

    dest : str
        Destination path
    """
    try:
        to_load.write.parquet(path, mode=mode)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to write output table to '{path}'. Reason: {exc}"
        ) from exc


def run(config_path: str):
    """Executes the ETL pipeline based on the provided configuration.

    Parameters
    ----------
    config_path : str
        The path to the YAML configuration file.
    """

    config = ConfigFile.from_yaml(config_path)
    kwargs = config.get_task_args()

    chain = (
        extract.s(kwargs=kwargs["extract"])
        | transform.s(kwargs=kwargs["transform"])
        | load.s(kwargs=kwargs["load"])
    )

    chain().eval()
