from pyspark.sql import DataFrame
from tinytask.decorators import task

from sparkit import apply


@task()
def apply_many(
    data: DataFrame,
    serialized_appliers: list[apply.SerializedApplier],
) -> DataFrame:
    """Applies given `appliers` sequentially to `data`.

    `appliers` are applied in the given order.

    Parameters
    ----------
    data : DataFrame
        Input data.

    appliers : list of SerializedApplier
        Functions to apply.
    """
    serialized_appliers = map(apply.to_factory_args, serialized_appliers)

    applier_instances = []
    for applier in serialized_appliers:
        applier = apply.factory.create(applier["name"], **applier["args"])
        applier_instances.append(applier)

    if not applier_instances:
        raise ValueError("No valid appliers created.")

    return apply.make_apply_chain(applier_instances)(data)
