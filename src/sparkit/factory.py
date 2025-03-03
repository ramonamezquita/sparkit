from sparkit.registry import Registry


class Factory:
    """
    Factory class to instantiate objects dynamically from one or more registries.
    """

    def __init__(self):
        self._registry = Registry()

    def add_registry(self, registry: Registry):
        self._registry.merge(registry)

    def create(self, name: str, *args, **kwargs) -> object:
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
        for registry in self._registry:
            cls = self._registry.get(name)
            if cls:
                return cls(*args, **kwargs)
        raise KeyError(f"Class with name '{name}' not found in any registry.")


def create_factory(registries: list[Registry] = ()) -> Factory:
    factory = Factory()
    for registry in registries:
        factory.add_registry(registry)

    return factory
