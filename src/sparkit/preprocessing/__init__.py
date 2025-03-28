from sparkit.factory import create_factory
from sparkit.preprocessing import column, date, df, text
from sparkit.preprocessing.pipeline import make_pipeline

registries = [
    column.registry,
    date.registry,
    df.registry,
    text.registry,
]


factory = create_factory(registries)
