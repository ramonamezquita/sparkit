from sparkit.factory import create_factory
from sparkit.preprocessing import column, date
from sparkit.preprocessing.pipeline import make_pipeline

registries = [column.registry, date.registry]


factory = create_factory(registries)
