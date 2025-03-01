"""
Functions to apply on a list of dictionaries (objects).
"""

from sparkit.exceptions import MultipleObjectsReturned


def filter_(objects: list[object], **kwargs) -> list[object]:

    def filter_function(o) -> bool:
        return all(getattr(o, k) == v for k, v in kwargs.items())

    return list(filter(filter_function, objects))


def get(objects: list[object], default=None, **kwargs) -> object | None:
    filtered = filter_(objects, **kwargs)
    length = len(filtered)

    if length > 1:
        raise MultipleObjectsReturned(n=length)

    return default if length == 0 else filtered[0]
