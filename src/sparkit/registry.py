from __future__ import annotations

from typing import Callable


def gen_default_name(fun: Callable) -> str:
    """Generates default name from the given function."""
    name = fun.__name__
    module_name = fun.__module__
    return ".".join([module_name, name])


class Registry(dict):
    """Generic objects registry."""

    def __init__(
        self, prefix: str | None = None, exception: Exception = KeyError
    ):
        self.prefix = prefix or ""
        self.exception = exception

    def __missing__(self, key: str):
        self.raise_exception(key)

    def __call__(self, name: str | None = None) -> None:
        return self.register(name)

    def raise_exception(self, key: str):
        raise self.exception(key)

    def register(self, name: str | None = None) -> Callable:
        """Registers functions in the internal registry.

        Use it as decorator to register objects in the internal registry.

        Example
        -------
        registry = Registry()

        @registry()
            def foo():
                ...

        Parameters
        ----------
        name : str, default = None
            Name under which to store the function object.

        Returns
        -------
        inner : Callable
            Actual decorator that registers objects to the internal registry
            under the given name.
        """

        def inner(fun: Callable) -> Callable:
            name_ = name or gen_default_name(fun)
            self[self.prefix + name_] = fun
            return fun

        return inner

    def unregister(self, name: str):
        """Unregisters a function by name.

        Parameters
        ----------
        name : str
            Name of the function object to unregister

        Raises
        ------
        KeyError
        """
        try:
            self.pop(name)
        except KeyError:
            self.raise_exception(name)

    def merge(self, other: Registry) -> None:
        """Merges another registry into this one.

        Parameters
        ----------
        other : Registry
            Another registry instance to merge.

        Raises
        ------
        ValueError
            If there are duplicate keys between the registries.
        """
        duplicates = set(self.keys()) & set(other.keys())
        if duplicates:
            raise ValueError(f"Duplicate keys found: {duplicates}")

        self.update(other)
