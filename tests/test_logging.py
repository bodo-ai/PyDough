import inspect
import io
import logging
from contextlib import redirect_stdout

import pandas as pd
import pytest
from tpch_outputs import tpch_q1_output
from tpch_relational_plans import (
    tpch_query_1_plan,
)

from pydough.database_connectors import DatabaseContext
from pydough.logger import get_logger
from pydough.relational import RelationalRoot
from pydough.sqlglot import SqlGlotTransformBindings, execute_df

pytestmark = [pytest.mark.execute]

def get_function_name():
    return inspect.currentframe().f_back.f_code.co_name


def test_get_logger_default_level(capsys):
    """
    Test logger with the default logging level and output.
    """
    logger = get_logger(get_function_name()+"_test_logger")
    logger.info("This is an INFO message")
    captured = capsys.readouterr()
    assert "INFO" in captured.out
    assert "This is an INFO message" in captured.out


def test_get_logger_env_override(monkeypatch, capsys):
    """
    Test overriding the log level using an environment variable.
    """
    # Set the environment variable to override the log level
    monkeypatch.setenv("PYDOUGH_LOG_LEVEL", "DEBUG")
    logger = get_logger(get_function_name()+"_test_logger")
    logger.debug("This is a DEBUG message")
    captured = capsys.readouterr()
    assert "DEBUG" in captured.out
    assert "This is a DEBUG message" in captured.out


def test_get_logger_custom_handler(monkeypatch,capsys):
    """
    Test logger with a custom handler.
    """
    # Create a custom handler
    custom_handler = logging.StreamHandler()
    custom_handler.setLevel(logging.ERROR)
    monkeypatch.setenv("PYDOUGH_LOG_LEVEL", "ERROR")
    # Create the logger with the custom handler
    logger = get_logger(get_function_name()+"_test_logger", handlers=[custom_handler])
    # Log messages of different levels
    logger.warning("This is a WARNING message")
    logger.error("This is an ERROR message")
    captured = capsys.readouterr()
    assert "WARNING" not in captured.out  # WARNING is below the custom handler's level (ERROR)
    assert "ERROR" in captured.out
    assert "This is an ERROR message" in captured.out


def test_get_logger_no_duplicate_handlers():
    """
    Test that duplicate handlers are not added to the logger.
    """
    logger = get_logger(get_function_name()+"_test_logger")
    # Store the initial number of handlers
    initial_handler_count = len(logger.handlers)
    # Call `get_logger` again with the same name
    logger = get_logger(get_function_name()+"_test_logger")
    assert len(logger.handlers) == initial_handler_count


def test_get_logger_format(capsys):
    """
    Test if the log format is correctly applied.
    """
    log_format = "%(levelname)s - %(message)s"
    logger = get_logger(get_function_name()+"_test_logger", fmt=log_format)
    logger.error("Test format")
    captured = capsys.readouterr()
    assert "ERROR - Test format" in captured.out


@pytest.mark.parametrize(
    "root, output",
    [
        pytest.param(
            tpch_query_1_plan(),
            tpch_q1_output(),
            id="tpch_q1",
        ),
    ],
)
def test_execute_df_logging(
    root: RelationalRoot,
    output: pd.DataFrame,
    sqlite_tpch_db_context: DatabaseContext,
    sqlite_bindings: SqlGlotTransformBindings,
) -> None:
    """
    Test the example TPC-H relational trees executed on a
    SQLite database, and capture any SQL or output printed to stdout.
    """
    # Create a StringIO buffer
    output_capture = io.StringIO()

    # Redirect stdout to the buffer
    with redirect_stdout(output_capture):
        execute_df(root, sqlite_tpch_db_context, sqlite_bindings, display_sql=True)

    # Retrieve the output from the buffer
    captured_output = output_capture.getvalue()

    required_op = '''[INFO] pydough.sqlglot.execute_relational: SQL query:\n, SELECT L_RETURNFLAG, L_LINESTATUS, SUM_QTY, SUM_BASE_PRICE, SUM_DISC_PRICE, SUM_CHARGE, CAST(SUM_QTY AS REAL) / COUNT_ORDER AS AVG_QTY, CAST(SUM_BASE_PRICE AS REAL) / COUNT_ORDER AS AVG_PRICE, CAST(SUM_DISCOUNT AS REAL) / COUNT_ORDER AS AVG_DISC, COUNT_ORDER FROM (SELECT L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY) AS SUM_QTY, SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(L_DISCOUNT) AS SUM_DISCOUNT, SUM(TEMP_COL0) AS SUM_DISC_PRICE, SUM(TEMP_COL1) AS SUM_CHARGE, COUNT() AS COUNT_ORDER FROM (SELECT L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_RETURNFLAG, L_LINESTATUS, TEMP_COL0, TEMP_COL0 * (1 + L_TAX) AS TEMP_COL1 FROM (SELECT L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS TEMP_COL0 FROM (SELECT L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS FROM (SELECT L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE FROM LINEITEM) WHERE L_SHIPDATE <= '1998-12-01'))) GROUP BY L_RETURNFLAG, L_LINESTATUS) ORDER BY L_RETURNFLAG, L_LINESTATUS
    '''
    assert required_op.strip() in captured_output.strip(), f"'{required_op.strip()}' not found in captured output: {captured_output.strip()}"
