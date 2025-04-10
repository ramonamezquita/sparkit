from typing import Any

from sparkit.registry import Registry


class Factory:
    """Factory class to instantiate objects dynamically from one or more registries.

    Only allows to register objects of the same type.
    """

    def __init__(self):
        self._registry = Registry()

    def add_registry(self, registry: Registry):
        self._registry.merge(registry)

    def get(self, name: str) -> object:
        """Returns object from internal registry."""
        return self._registry.get(name)

    def create(self, name: str, *args, **kwargs) -> Any:
        """Creates an instance of the registered class by name.

        Parameters
        ----------
        name : str
            Name of the registered class to instantiate.

        *args : tuple
            Positional arguments to pass to the class constructor.

        **kwargs : dict
            Keyword arguments to pass to the class constructor.

        Returns
        -------
        object
            An instance of the registered class.

        Raises
        ------
        KeyError
            If the name is not found in any registry.
        """

        cls = self._registry.get(name)
        if cls:
            return cls(*args, **kwargs)
        raise KeyError(f"Class with name '{name}' not found in any registry.")


def create_factory(registries: list[Registry] = ()) -> Factory:
    factory = Factory()

    for r in registries:
        factory.add_registry(r)

    return factory
