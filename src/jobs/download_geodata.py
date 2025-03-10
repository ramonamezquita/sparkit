"""
Script to download and process GeoNames geographical data for a specified country.

This script fetches a country-specific GeoNames dataset, extracts relevant location 
information (e.g., name, timezone, coordinates, administrative codes), and stores 
the processed data in Parquet format.
"""

import argparse
import io
import os
import zipfile
from tempfile import TemporaryDirectory

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

JOB_NAME = "DownloadGeoData"

MAIN_SCHEMA = StructType(
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


def _create_url(endpoint: str) -> str:
    """Creates GeoData URL."""
    base_url = "https://download.geonames.org/export/dump"
    return f"{base_url}/{endpoint}"


def _download_geodata(
    spark: SparkSession,
    iso_code: str,
    dest: str | None = None,
    removeprefix: str = "",
) -> DataFrame:

    if dest is None:
        dest = os.path.join(os.getcwd(), "geodata")

    if not iso_code.isalpha() or not (2 <= len(iso_code) <= 3):
        raise ValueError(
            "Invalid ISO code. Must be a two- or three-letter country code."
        )

    # main GeoData.
    try:
        zip_url = _create_url(f"{iso_code}.zip")
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
            schema=MAIN_SCHEMA,
            sep="\t",
            encoding="utf-8",
        )

        prefixed_name = (
            F.col("name")
            .substr(F.lit(len(removeprefix)), F.length("name"))
            .alias("name")
        )

        select_cols = [
            prefixed_name,
            "timezone",
            "latitude",
            "longitude",
            "admin1code",
            "admin2code",
            "featurecode",
        ]

        maindata = maindata.where(
            F.col("featurecode").isin(["ADM1", "ADM2"])
        ).select(*select_cols)

        maindata.write.parquet(dest)


def run(iso_code: str, dest: str | None = None, removeprefix: str = "") -> None:

    spark: SparkSession = SparkSession.builder.appName(JOB_NAME).getOrCreate()
    _download_geodata(
        spark,
        iso_code=iso_code,
        dest=dest,
        removeprefix=removeprefix,
    )


# ---------------------- CLI ---------------------- #


def create_parser() -> argparse.ArgumentParser:
    """Creates an argument parser for CLI execution.

    Returns
    -------
    argparse.ArgumentParser
        The argument parser instance.
    """
    parser = argparse.ArgumentParser(
        description="Download GeoNames geographical data for a specified country."
    )

    parser.add_argument(
        "--iso_code",
        required=True,
        help="ISO code as displayed in https://download.geonames.org/export/dump/",
        type=str,
    )

    parser.add_argument(
        "--dest",
        help="Download destination. Can also be a URI (s3://, gcs://, etc). If None, a directory called `geodata` will be created inside the cwd.",
        type=str,
    )

    parser.add_argument(
        "--removeprefix",
        help="Prefix to remove from the `name` column.",
        type=str,
    )

    return parser


def cli() -> None:
    """Runs the pipeline as a CLI command."""
    parser = create_parser()
    args, _ = parser.parse_known_args()
    run(iso_code=args.iso_code, dest=args.dest, removeprefix=args.removeprefix)


if __name__ == "__main__":
    cli()
