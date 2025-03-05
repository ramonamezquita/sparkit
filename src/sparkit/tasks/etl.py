from typing import TypedDict

from pyspark.sql import DataFrame, SparkSession
from tinytask.decorators import task

from sparkit import extractors, preprocessing


class Source(TypedDict):
    """Represent a data source."""

    name: str
    filepath: str
    format: str
    options: dict


class Target(TypedDict):
    """Represents a data target."""

    path: str
    format: str
    options: dict
    mode: str = "overwrite"


class Transformer(TypedDict):
    """Represents a transformer object"""

    name: str
    kwargs: dict


@task()
def extract_many(
    sources: list[Source], spark: SparkSession
) -> dict[str, DataFrame]:
    """Extracts data from multiple sources.

    This function takes a list of data sources and dynamically instantiates
    the appropriate extractor class for each source based on its format. The
    extracted data is returned as a dictionary where the keys correspond to
    the `key` specified in the source definitions.

    Parameters
    ----------
    sources : list[Source]
        A list of source definitions, where each source is a dictionary with
        the following keys.

    spark : SparkSession
        The active SparkSession used for reading data.

    Returns
    -------
    dict[str, DataFrame]
        A dictionary where the keys are the source `key` values and the values
        are the corresponding extracted DataFrames.
    """
    extraction = {}

    for source in sources:
        try:
            kwargs = {
                "name": source["format"],
                "spark": spark,
                "filepath": source["filepath"],
                "options": source["options"],
            }
            extractor = extractors.factory.create(**kwargs)
            extraction[source["name"]] = extractor.extract()
        except Exception as e:
            raise RuntimeError(
                f"Failed to extract data for source '{source['name']}' "
                f"with format '{source['format']}': {e}"
            )

    return extraction


@task()
def load_many(data: dict[str, DataFrame], targets: dict[str, Target]) -> None:
    """Loads data into target destinations.

    Parameters
    ----------
    data : dict[str, DataFrame]
        A dictionary dataframes. Keys must match with `targets` keys.

    targets : dict[str, M.Target]
        A dictionary of target configurations.
    """
    for key, target in targets.items():
        try:
            sdf = data[key]
        except KeyError:
            raise KeyError(
                f"Missing dataset: No data found for '{key}'. "
                f"Available datasets are: {list(data)}."
            )

        try:
            sdf.write.parquet(
                target["path"], mode=target.get("mode", "overwrite")
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to write data '{key}' to '{target['path']}': {e}"
            ) from e


@task()
def transform_many(
    data: DataFrame, transformers: list[Transformer]
) -> DataFrame:
    """Applies given `transformers` sequentially to `data`.

    Transformers are applied in the given order.

    Parameters
    ----------
    data : DataFrame
        Input data.

    transformer : list of Transformer
        Transformers to apply.
    """

    transformers = preprocessing.factory.create_many(transformers)
    if not transformers:
        raise ValueError("No valid transformers created")

    return preprocessing.make_pipeline(transformers)(data)


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
