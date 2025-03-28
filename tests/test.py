from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

input_uri = "s3a://coppel-staging-eunorth1-local/dim_stores"
spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")


input_tb = spark.read.parquet(input_uri)
standard_tb = spark.read.parquet("/home/jovyan/GeoNamesDownload/")


# ADM1 Standardization
admin1_tb = (
    standard_tb.filter(F.col("featurecode") == "ADM1")
    .withColumn("name", F.regexp_replace("name", "Estado de ", ""))
    .select("name", "admin1code")
).alias("admin1_tb")

admin1_lev_tb = (
    admin1_tb.select("name")
    .crossJoin(input_tb.select("admin1name").distinct().alias("input_tb"))
    .withColumn("lev", F.levenshtein("admin1_tb.name", "input_tb.admin1name"))
)

w = Window.partitionBy("admin1name").orderBy("lev")

best_admin1 = (
    admin1_lev_tb.withColumn("rank", F.row_number().over(w))
    .filter(F.col("rank") == 1)
    .select("input_tb.admin1name", "admin1_tb.name")
)
