def split_commas(
    string: str, cast_to_int: bool = True
) -> list[int | str] | None:
    if not string or string is None or string == "None":
        return None

    split = string.split(",")

    if cast_to_int:
        return list(map(int, split))

    return split
