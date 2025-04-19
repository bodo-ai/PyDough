"""
Verifies that various documentation files are up to date.
"""

import pydough.pydough_operators as pydop


def test_function_list():
    """
    Tests that every function in the operator registry is also part of the
    `functions.md` file, unless it is a special name that should not be
    mentioned or it is a binary operator.

    Note: this test should only be run from the root directory of the project.
    """
    # Identify every function that should be documented
    special_names = {
        "NOT",
        "SLICE",
        "POPULATION_STD",
        "POPULATION_VARIANCE",
        "SAMPLE_STD",
        "SAMPLE_VARIANCE",
    }
    function_names: set[str] = set()
    for function_name, operator in pydop.builtin_registered_operators().items():
        if not (
            isinstance(operator, pydop.BinaryOperator) or function_name in special_names
        ):
            function_names.add(function_name)
    # Identify every section header in the function documentation
    headers: set[str] = set()
    with open("documentation/functions.md") as f:
        for line in f.readlines():
            if line.startswith("#"):
                headers.add(line.strip("#").strip())
    # Remove any function name that is in the headers, and fail if there are
    # any that remain
    function_names.difference_update(headers)
    if function_names:
        raise Exception(
            "The following functions are not documented: " + ", ".join(function_names)
        )
