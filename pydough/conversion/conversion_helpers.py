"""
TODO
"""

__all__ = ["build_new_ref_name"]


def build_new_ref_name(name: str, idx: int) -> str:
    """
    Build a new reference name by appending an index suffix.

    For quoted identifiers, the suffix is added to the identifier content
    before the closing quote. Escaped quote characters inside the identifier
    are preserved.

    Args:
        `name`: Identifier that is being modified
        `idx`: Index being added to the identifier

    Returns:
        The new ref name with the index added

    Examples:
        name       -> name_1
        "column name" -> "column name_1"
        "column ""name""\" -> "column ""name""_1"
        `column` -> `column_1`

    """
    if len(name) >= 2:
        quote = name[0]

        if quote == name[-1] and quote in {'"', "`"}:
            return f"{name[:-1]}_{idx}{quote}"

    return f"{name}_{idx}"
