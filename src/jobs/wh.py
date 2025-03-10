"""
Configuration-oriented star schema ETL pipeline.

This script defines an ETL (Extract-Transform-Load) pipeline for 
creating a star schema using PySpark. It leverages the `sparkit` 
library for task management, configuration handling, and logging, 
and it integrates PySpark's ML Pipeline API for preprocessing and 
transformation. The whole process is: 
extract -> preprocess -> transform -> validate -> load.

Command-Line Interface (CLI):
This script includes a CLI entry point that allows it to be executed 
directly with Python or using `spark-submit`. For example,

```bash
python starSchema.py --config_path /path/to/config.yaml
```

Or with `spark-submit`,

```bash
spark-submit --py-files /path/to/sparkit.zip starSchema.py --config_path /path/to/config.yaml
```
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from tinytask.callbacks import LoggingCallback
from tinytask.configfile import ConfigFile
from tinytask.decorators import task

from sparkit import objects as O
from sparkit import wh
from sparkit.logging import create_default_logger
from sparkit.tasks import etl

JOB_NAME = "WarehouseETL"


__logger__ = create_default_logger(name=JOB_NAME)


@task(name="Extract", callbacks=[LoggingCallback(__logger__)])
def extract(sources: list[etl.Source]) -> O.ObjectStore:
    """Extracts data from multiple sources."""
    spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()

    os = O.ObjectStore()
    extracted = etl.extract_many(sources, spark)
    os.add(key="extracted", value=extracted)
    return os


@task(name="Preprocess", callbacks=[LoggingCallback(__logger__)])
def preprocess(
    os: O.ObjectStore, transformations: dict[str, list[etl.Transformer]]
) -> O.ObjectStore:
    """Preprocess extracted data."""
    extracted = os.get("extracted")
    preprocessed = {}

    for key, trans in transformations.items():
        preprocessed[key] = etl.transform_many(extracted[key], trans)

    os.add(key="preprocessed", value=preprocessed)

    return os


@task(name="Transform", callbacks=[LoggingCallback(__logger__)])
def transform(
    os: O.ObjectStore,
    metadata: list[wh.Metadata],
) -> O.ObjectStore:
    """Transforms preprocessed data into a star schema."""
    data = os.get("preprocessed").pop("raw")

    # Build warehouse.
    # Data is transformed into a fact table containing foreign
    # keys to dimension tables. Fact and dimension tables defininition
    # are given through the `metadata` parameter.
    transformed = wh.build(metadata, data)
    os.add(key="transformed", value=transformed)

    return os


# @task(name="Validate", callbacks=[LoggingCallback(__logger__)])
# def validate(
#    os: ObjectStore,
#    validations: dict[str, list[M.Validation]],
# ) -> dict[str, DataFrame]:
#    """Validates transformed data.
#    """
#    validated = {}
#    transformed = registry.get("transformed")
#    args_dispatcher = ArgsDispatcher(**transformed)
#
#    for name, vals in validations.items():
#        validated[name] = etl.validate_many(
#            transformed[name], vals, args_dispatcher
#        )
#
#    return validated


@task(name="Load", callbacks=[LoggingCallback(__logger__)])
def load(
    os: O.ObjectStore,
    targets: dict[str, etl.Target],
) -> None:
    """Loads validated data into target destinations.

    Parameters
    ----------
    validated : dict[str, DataFrame]
        A dictionary of validated data.

    targets : dict[str, str]
        A dictionary of target configurations.
    """
    _, to_load = os.get_last()
    etl.load_many(to_load, targets)


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
        | preprocess.s(kwargs=kwargs["preprocess"])
        | transform.s(kwargs=kwargs["transform"])
        | load.s(kwargs=kwargs["load"])
    )

    chain().eval()


# ---------------------- CLI ---------------------- #


def create_parser() -> argparse.ArgumentParser:
    """Creates an argument parser for CLI execution.

    Returns
    -------
    argparse.ArgumentParser
        The argument parser instance.
    """
    parser = argparse.ArgumentParser(description="Warehouse ETL.")
    parser.add_argument(
        "--config_path", "-C", help="Path to YAML configuration file."
    )
    return parser


def cli() -> None:
    """Runs the pipeline as a CLI command."""
    parser = create_parser()
    args, _ = parser.parse_known_args()
    run(config_path=args.config_path)


if __name__ == "__main__":
    cli()
