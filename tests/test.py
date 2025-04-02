from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField
from pyspark.sql.window import Window

from sparkit.utils.spark import check_fields


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
            "input_tb.admin1name", F.col("standard_tb.name").alias("stdname")
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
            "timezone"
        )
    )

    return admin2_map


input_uri = "s3a://coppel-staging-eunorth1-local/dim_stores"
spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")


input_tb = spark.read.parquet(input_uri)
standard_tb = spark.read.parquet("/home/jovyan/GeoNamesDownload/").alias(
    "standard_tb"
)

# Check input fields
fields = [
    StructField("admin1name", StringType()),
    StructField("admin2name", StringType()),
]
check_fields(input_tb, fields)

# ADM1 Standardization
admin1_map = _create_admin1_map(input_tb, standard_tb)
input_tb = (
    input_tb.join(admin1_map, "admin1name")
    .drop("admin1name")
    .withColumnRenamed("stdname", "admin1name")
)

# ADM2 Standardization
admin2_map = _create_admin2_map(standard_tb, input_tb)
input_tb = (
    input_tb.join(admin2_map, ["admin1name", "admin2name"])
    .drop("admin2name")
    .withColumnRenamed("stdname", "admin2name")
)
