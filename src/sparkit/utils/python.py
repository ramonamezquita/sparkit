def unnest(d: dict, keys: list | None = None) -> list[tuple]:
    """Recursively unnest dictionary."""
    keys = keys or []
    result = []
    for k, v in d.items():
        if isinstance(v, dict):
            result.extend(unnest(v, keys + [k]))
        else:
            result.append(tuple(keys + [k, v]))

    return result


def check_keys(d: dict, keys: list[str], alias: str = "") -> None:
    for k in keys:
        if k not in d:
            raise ValueError(f"Missing key `{k}` in dict `{alias}` ")
