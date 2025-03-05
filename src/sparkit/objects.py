"""
Functions to apply on a list of dictionaries (objects).
"""

from collections import OrderedDict
from typing import Any

from sparkit.exceptions import MultipleObjectsReturned


class ObjectStore:
    def __init__(self):
        self._root = OrderedDict()

    @property
    def root(self):
        return self._root

    def add(self, key: str, value: dict[str, Any]) -> None:
        """Adds a dictionary to the storage."""
        self._root[key] = value

    def get(self, key: str) -> dict[str, Any] | None:
        """Retrieves a dictionary by its ID."""
        return self._root.get(key)

    def clear(self) -> None:
        """Removes all stored dictionaries."""
        self._root.clear()

    def exists(self, key: str) -> bool:
        """Checks if a dictionary with the given ID exists."""
        return key in self._root

    def get_last(self) -> tuple[str, dict]:
        k = next(reversed(self.root))
        v = self.root[k]
        return k, v

    def get_flattened(self) -> dict[str, Any]:
        """Returns all nested dict values in a single dict with keys prefixed
        by their dict ID."""
        flattened = {}
        for key, value in self._root.items():
            for inner_key, inner_value in value.items():
                flattened[f"{key}.{inner_key}"] = inner_value
        return flattened


def filter_(objects: list[dict], **kwargs) -> list[dict]:
    """Filters a list of dictionaries based on key-value conditions."""

    def filter_function(d: dict) -> bool:
        return all(d.get(k) == v for k, v in kwargs.items())

    return list(filter(filter_function, objects))


def get(objects: list[dict], default=None, **kwargs) -> dict | None:
    """Retrieves a single dictionary from a list based on key-value conditions."""
    filtered = filter_(objects, **kwargs)
    length = len(filtered)

    if length > 1:
        raise MultipleObjectsReturned(n=length)

    return default if length == 0 else filtered[0]
