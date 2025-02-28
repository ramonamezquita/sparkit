from sparkit.factory import create_factory
from sparkit.preprocessing import column, date

registries = [column.registry, date.registry]


factory = create_factory(registries)
