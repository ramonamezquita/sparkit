from sparkit.factory import create_factory
from sparkit.preprocessing import column, dataframe, date, text

registries = [
    column.registry,
    date.registry,
    dataframe.registry,
    text.registry,
]


factory = create_factory(registries)
