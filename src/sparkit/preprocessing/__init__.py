from pipeline import make_pipeline

from sparkit.factory import create_factory
from sparkit.preprocessing import column, date

registries = [column.registry, date.registry]


factory = create_factory(registries)
