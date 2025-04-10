from typing import TypedDict

from pyspark.sql import DataFrame, SparkSession
from tinytask.decorators import task

from sparkit import sources as ss


class SerializedTarget(TypedDict):
    """Represents a data target."""

    path: str
    format: str
    options: dict
    mode: str = "overwrite"


@task()
def extract_many(
    sources: list[ss.SerializedSource], spark: SparkSession
) -> dict[str, DataFrame]:
    """Extracts data from multiple sources.

    This function takes a list of data sources and dynamically instantiates
    the appropriate extractor class for each source based on its format. The
    extracted data is returned as a dictionary where the keys correspond to
    the `key` specified in the source definitions.

    Parameters
    ----------
    sources : list[sources.SerializedSource]
        A list of source definitions.

    spark : SparkSession
        The active SparkSession used for reading data.

    Returns
    -------
    dict[str, DataFrame]
        A dictionary where the keys are the source `key` values and the values
        are the corresponding extracted DataFrames.
    """
    sourced = {}

    for source in sources:
        try:

            kwargs = {
                "name": source["format"],
                "spark": spark,
                "filepath": source["filepath"],
                "options": source.get("options"),
            }
            source_instance: ss.Source = ss.factory.create(**kwargs)
            sourced[source["name"]] = source_instance.extract()
        except Exception as e:
            raise RuntimeError(
                f"Failed to extract data for source '{source['name']}' "
                f"with format '{source['format']}': {e}"
            )

    return sourced


@task()
def load_many(
    data: dict[str, DataFrame], targets: list[SerializedTarget]
) -> None:
    """Loads data into target destinations.

    Parameters
    ----------
    data : dict[str, DataFrame]
        A dictionary dataframes. Keys must match with `targets` keys.

    targets : dict[str, M.Target]
        A dictionary of target configurations.
    """
    for target in targets:
    
        try:
            name = target["name"]
            sdf = data[name]
        except KeyError:
            raise KeyError(
                f"Missing dataset: No data found for '{name}'. "
                f"Available datasets are: {list(data)}."
            )

        try:
            sdf.write.parquet(
                target["path"], mode=target.get("mode", "overwrite")
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to write data '{name}' to '{target['path']}': {e}"
            ) from e


# @task()
# def validate_many(
#    data: DataFrame,
#    validations: list,
#    args_dispatcher:,
# ) -> None:
#
#    for v in validations:
#       try:
#
#            cls = validators.factory.get(v.name)
#            validator = args_dispatcher.init(cls, **v.args)
#            return validator.validate(data)
#
#        except Exception as exc:
#            clsname = validator.__class__.__name__
#            raise RuntimeError(
#                f"Validation failed in {clsname}:\n"
#                f"- DataFrame: {data}\n"
#                f"- Reason: {exc}"
#            )
