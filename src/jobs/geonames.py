import argparse
import io
import os
import zipfile
from tempfile import TemporaryDirectory

import pandas as pd
import requests

COLNAMES = [
    "geonameid",
    "name",
    "asciiname",
    "alternatenames",
    "latitude",
    "longitude",
    "feature class",
    "feature code",
    "country code",
    "cc2",
    "admin1 code",
    "admin2 code",
    "admin3 code",
    "admin4 code",
    "population",
    "elevation",
    "dem",
    "timezone",
    "modification date",
]

ADMIN_COLNAMES = [
    "code",
    "name",
    "asciiname",
    "geonameid",
]

DUMP_URL = "https://download.geonames.org/export/dump"


def _download_admin_codes(url: str, colnames: list) -> pd.DataFrame:
    try:
        return pd.read_csv(url, sep="\t", names=colnames, encoding="utf-8")
    except pd.errors.ParserError as e:
        raise RuntimeError(f"Parsing error in {url}: {e}")
    except OSError as e:
        raise RuntimeError(f"File I/O error for {url}: {e}")


def run(iso_code: str):

    if not iso_code.isalpha() or not (2 <= len(iso_code) <= 3):
        raise ValueError(
            "Invalid ISO code. Must be a two- or three-letter country code."
        )

    zip_url = f"{DUMP_URL}/{iso_code}.zip"
    admin1_url = f"{DUMP_URL}/admin1CodesASCII.txt"
    admin2_url = f"{DUMP_URL}/admin2Codes.txt"

    try:
        r = requests.get(zip_url, timeout=10)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f"Network error while downloading GeoNames dump: {e}"
        )
    except zipfile.BadZipFile:
        raise RuntimeError("Downloaded file is not a valid ZIP archive.")

    with TemporaryDirectory() as tmpdir:
        z.extractall(tmpdir)
        filepath = os.path.join(tmpdir, iso_code + ".txt")

        if not os.path.exists(filepath):
            raise FileNotFoundError(
                f"Expected file {filepath} not found after extraction."
            )

        geodata = pd.read_csv(
            filepath,
            sep="\t",
            names=COLNAMES,
            encoding="utf-8",
            low_memory=False,
        )

    admin1codes = _download_admin_codes(admin1_url, ADMIN_COLNAMES)
    admin2codes = _download_admin_codes(admin2_url, ADMIN_COLNAMES)


def create_parser() -> argparse.ArgumentParser:
    """Creates an argument parser for CLI execution.

    Returns
    -------
    argparse.ArgumentParser
        The argument parser instance.
    """
    parser = argparse.ArgumentParser(description="Download GeoNames dump.")
    parser.add_argument(
        "--iso_code",
        required=True,
        help="ISO code as displayed in https://download.geonames.org/export/dump/",
    )
    return parser


def cli() -> None:
    """Runs the pipeline as a CLI command."""
    parser = create_parser()
    args, _ = parser.parse_known_args()
    run(iso_code=args.iso_code)


if __name__ == "__main__":
    cli()
