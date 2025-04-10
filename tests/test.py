from __future__ import annotations

from pyspark.sql import SparkSession
from tinytask.configfile import ConfigFile

from src.jobs.buildWareHouse import apply, extract

spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

config_path = "/home/jovyan/config/buildWH2"

config = ConfigFile.from_yaml(config_path)

kwargs = config.get_task_args()

os = extract(**kwargs["extract"])
raw = os.get("sourced")["raw"]
applied = apply(os, **kwargs["apply"])