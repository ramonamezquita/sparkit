from pyspark.ml import Pipeline
from pyspark.sql import DataFrame, SparkSession
from tinytask.decorators import task

from sparkit import models as M
from sparkit import preprocessing


@task()
def extract_many(
    sources: list[M.Source], spark: SparkSession
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

    Raises
    ------
    - KeyError
        If no extractor is registered for a given format.
    - Exception
        If the extractor fails to load the data for any source.
    """
    extraction = {}

    for source in sources:
        try:
            kwargs = {
                "name": source.format,
                "spark": spark,
                "filepath": source.filepath,
                "options": source.options,
            }
            extractor = extractors.factory.create(**kwargs)
            extraction[source.key] = extractor.extract()
        except Exception as e:
            raise RuntimeError(
                f"Failed to extract data for source '{source.key}' "
                f"with format '{source.format}': {e}"
            )

    return extraction


@task()
def load_many(data: dict[str, DataFrame], targets: dict[str, M.Target]) -> None:
    """Loads data into target destinations.

    Parameters
    ----------
    data : dict[str, DataFrame]
        A dictionary of validated data, where keys are table names,
        and values are DataFrames.

    targets : dict[str, M.Target]
        A dictionary of target configurations.

    Raises
    ------
    KeyError
        If a table name in `targets` is missing from `tables`.

    RuntimeError
        If an error occurs while writing a table to its destination.
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
            sdf.write.parquet(target.path, mode="overwrite")
        except Exception as e:
            raise RuntimeError(
                f"Failed to write data '{key}' to '{target.path}': {e}"
            ) from e


@task()
def transform_many(data: DataFrame, transformations: list[M.Generic]):

    stages = preprocessing.factory.create_many(transformations)
    if not stages:
        raise ValueError("No valid transformation stages created")

    pipeline = Pipeline(stages=stages)
    model = pipeline.fit(data)
    return model.transform(data)


@task()
def validate_many(
    data: DataFrame,
    validations: list[M.Validation],
    args_dispatcher: ArgsDispatcher,
) -> None:

    for v in validations:
        try:

            cls = validators.factory.get(v.name)
            validator = args_dispatcher.init(cls, **v.args)
            return validator.validate(data)

        except Exception as exc:
            clsname = validator.__class__.__name__
            raise RuntimeError(
                f"Validation failed in {clsname}:\n"
                f"- DataFrame: {data}\n"
                f"- Reason: {exc}"
            )
