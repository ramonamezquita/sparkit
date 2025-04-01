from __future__ import annotations

from typing import TypedDict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkit.utils.spark import assign_ids, deduplicate


class Metadata(TypedDict):
    name: str
    id_cols: list[str]
    merge_on: str | None


def _distinct(data: DataFrame, cols: list[str]) -> DataFrame:
    """Distinct table factory.

    Creates a "distinct" table by deduplicating data and assigning unique IDs.
    """
    dim = deduplicate(data, cols)
    dim = assign_ids(dim, id_col="id")
    return dim


def _merge(
    left: DataFrame,
    right: DataFrame,
    metadata: Metadata,
) -> DataFrame:
    """Joins a dimension table with raw data and extracts the dimension column."""
    if "id" not in right.columns:
        raise ValueError("`right` must contain 'id' column for merge.")

    new_id_col = f"{metadata['name']}_id"
    right = right.withColumnRenamed("id", new_id_col)
    right = F.broadcast(right.select(new_id_col, metadata["merge_on"]))
    merged = left.join(right, on=metadata["merge_on"], how="inner")
    merged = merged.drop(*metadata["id_cols"])

    return merged


def build(metadata: list[Metadata], data: DataFrame):
    """Auxiliary for building a warehouse schema.

    This function allows you to create a star schema by providing a list of
    dimension metadata and a pre-existing fact table.

    Parameters
    ----------
    metadata : list[TableMetadata]
        A list of TableMetadata objects, each containing information about a
        dimension table to be added to the schema.

    data : DataFrame
        The fact table to be used in the schema.
    """
    builder = WarehouseBuilder(data)

    for m in metadata:
        builder.add_dim(m)

    return builder.build()


class WarehouseBuilder:
    """Builder class for creating a warehouse schema.

    This class provides methods to define a fact table and incrementally
    add dimension tables to construct a warehouse schema. It is built
    lazily, leveraging PySpark's computational graph.

    Parameters
    ----------
    data : DataFrame
        Fact table from which dimensions will be created and merged
        using surrogate keys.
    """

    def __init__(self, main):
        # Mapping from name (str) to warehouse table (DataFrame).
        self._name_to_data = {"main": main}

    @property
    def main(self) -> DataFrame:
        return self._name_to_data["main"]

    def add_dim(self, metadata: Metadata) -> WarehouseBuilder:
        """Adds a dimension table to the warehouse schema.

        This method uses the provided dimension metadata to create a dimension
        table and joins it with the current main table. The join operation is
        lazily evaluated, adding to the computational graph without immediate
        execution. The main table is updated with the join, and the dimension
        table is stored for later retrieval.

        Parameters
        ----------
        metadata : Metadata
            Metadata for the dimension table.

        Returns
        -------
        self (object)
        """
        new_dim = _distinct(self.main, cols=metadata["id_cols"])

        updated_main = _merge(
            left=self.main,
            right=new_dim,
            metadata=metadata,
        )

        self._name_to_data["main"] = updated_main
        self._name_to_data[metadata["name"]] = new_dim

        return self

    def build(self) -> dict[str, DataFrame]:
        """Builds the star schema with the specified raw data."""
        return self._name_to_data
