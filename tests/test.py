from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField
from pyspark.sql.window import Window

from sparkit.utils.spark import check_fields


def _create_admin1_map(
    input_tb: DataFrame, standard_tb: DataFrame
) -> DataFrame:
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

    return admin1_map, admin1_tb


def _create_admin2_map(
    standard_tb: DataFrame, admin1_tb: DataFrame, input_tb: DataFrame
) -> DataFrame:
    """Creates a mapping between input names and standard names using
    Levenshtein distance for best matching.

    Returns
    -------
    DataFrame
        A mapping DataFrame with input admin2 names and their closest
        standardized match.
    """
    admin2_filter = F.col("featurecode") == "ADM2"

    admin2_tb = (
        standard_tb.filter(admin2_filter)
        .join(admin1_tb, "admin1code")
        .select(
            F.col("standard_tb.name").alias("name"),
            F.col("admin1_tb.name").alias("admin1name"),
        )
    ).alias("admin2_tb")

    lev_col = F.levenshtein("admin2_tb.name", "input_tb.admin2name")
    admin2_tb = admin2_tb.join(
        input_tb.select("admin1name", "admin2name")
        .distinct()
        .alias("input_tb"),
        "admin1name",
    ).withColumn("lev", lev_col)

    window = Window.partitionBy("admin1name", "admin2name").orderBy("lev")
    admin2_map = (
        admin2_tb.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank", "lev")
        .select("admin1name", "admin2name", F.col("name").alias("stdname"))
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
admin1_map, admin1_tb = _create_admin1_map(input_tb, standard_tb)
input_tb = (
    input_tb.join(admin1_map, "admin1name")
    .drop("admin1name")
    .withColumnRenamed("name", "admin1name")
)

# ADM2 Standardization
admin2_map = _create_admin2_map(standard_tb, admin1_tb, input_tb)
#admin2_map.write.csv("/workspace/admin2map.csv")
