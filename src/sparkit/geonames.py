import io
import os
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def create_schema() -> StructType:

    return StructType(
        [
            StructField("geonameid", IntegerType(), False),
            StructField("name", StringType()),
            StructField("asciiname", StringType()),
            StructField("alternatenames", StringType()),
            StructField("latitude", StringType()),
            StructField("longitude", StringType()),
            StructField("featureclass", StringType()),
            StructField("featurecode", StringType()),
            StructField("countrycode", StringType()),
            StructField("cc2", StringType()),
            StructField("admin1code", StringType()),
            StructField("admin2code", StringType()),
            StructField("admin3code", StringType()),
            StructField("admin4code", StringType()),
            StructField("popoulation", StringType()),
            StructField("elevation", StringType()),
            StructField("dem", StringType()),
            StructField("timezone", StringType()),
            StructField("modificationdate", StringType()),
        ]
    )


def create_url(endpoint: str = "") -> str:
    """Creates GeoData URL."""
    base_url = "https://download.geonames.org/export/dump"
    return f"{base_url}/{endpoint}"


def download_geonames(
    iso_code: str,
    dest: str | None = None,
    extra_cols: list[str] = (),
    spark: SparkSession | None = None,
) -> Path:
    """

    Parameters
    ----------
    iso_code : str
        ISO code as displayed in https://download.geonames.org/export/dump/

    dest : str
        Download destination. Can also be a URI (s3://, gcs://, etc). If None,
        a directory called `geodata` will be created inside the cwd.

    extra_cols : list of str, default=()
        Extra columns from GeoName data. See
        https://download.geonames.org/export/dump/ for the full list.
        By default, only the following are saved:
        ["name", "timezone", "latitude", "longitude", "admin1code",
        "admin2code", "featurecode"]

    spark : SparkSession, default=None
        Active Spark session to use. If None, a new one will be created.
    """

    if dest is None:
        dest = os.path.join(os.getcwd(), "geonames")
    else:
        dest = os.fspath(dest)

    if not iso_code.isalpha() or not (2 <= len(iso_code) <= 3):
        raise ValueError(
            "Invalid ISO code. Must be a two or three-letter country code."
        )

    try:
        zip_url = create_url(f"{iso_code}.zip")
        r = requests.get(zip_url, timeout=10)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f"Network error while downloading GeoNames dump: {e}"
        )
    except zipfile.BadZipFile:
        raise RuntimeError("Downloaded file is not a valid ZIP archive.")

    with TemporaryDirectory() as tempdir:

        z.extractall(tempdir)
        filepath = os.path.join(tempdir, iso_code + ".txt")

        if not os.path.exists(filepath):
            raise FileNotFoundError(
                f"Expected file {filepath} not found after extraction."
            )

        maindata = spark.read.csv(
            filepath,
            schema=create_schema(),
            sep="\t",
            encoding="utf-8",
        )

        select_cols = [
            "name",
            "timezone",
            "latitude",
            "longitude",
            "admin1code",
            "admin2code",
            "featurecode",
        ]

        select_cols += list(extra_cols)
        maindata.select(*select_cols).write.parquet(dest)

        return Path(dest)
