from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sparkit.geonames import download_geonames
from sparkit.utils import deduplicate


def get_standard_tb(spark: SparkSession = None) -> DataFrame:
    if spark is None:
        spark: SparkSession = SparkSession.builder.getOrCreate()

    download_dest = "GeoNamesDownload"
    download_geonames("MX", dest=download_dest, spark=spark)
    geonames = spark.read.parquet(download_dest).where(
        F.col("featurecode").isin("ADM1", "ADM2")
    )

    admin1code_to_name = {
        r["admin1code"]: r["name"]
        for r in geonames.where(F.col("featurecode") == "ADM1")
        .select(
            F.col("name").substr(F.lit(10), F.length("name")).alias("name"),
            "admin1code",
        )
        .collect()
    }

    select_cols = [
        F.col("admin1code").alias("state"),
        F.col("name").alias("city"),
        "latitude",
        "longitude",
        "timezone",
    ]

    standard_tb = (
        geonames.where(F.col("featurecode") == "ADM2")
        .replace(admin1code_to_name, subset="admin1code")
        .select(select_cols)
    )

    return standard_tb


def run(input_uri: str, output_uri: str):

    spark: SparkSession = SparkSession.builder.getOrCreate()
    standard_tb = get_standard_tb(spark)
    standard_cities = [
        r.city for r in deduplicate(standard_tb, ["city"]).collect()
    ]
    

    input_tb: DataFrame = get_extractor(input_uri).extract()

    input_tb = TextNormalizer(cols=["state", "city"], initcap=True).transform(
        input_tb
    )

    # Standardize input table.

    # Merge
