import logging
import subprocess
import sys
from io import StringIO

import pytest

from pydough.configs import PyDoughSession
from pydough.logger import get_logger
from pydough.sqlglot import execute_df
from tests.test_pydough_functions.tpch_relational_plans import (
    tpch_query_1_plan,
)

pytestmark = [pytest.mark.execute]


def test_get_logger_default_level():
    """
    Test logger with the default logging level and output.
    """
    code = r"""
from pydough.logger import get_logger

logger = get_logger("default_level_test_logger")
logger.info("This is an INFO message")
"""
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert "[INFO]" in p.stdout
    assert "This is an INFO message" in p.stdout


def test_get_logger_env_override():
    """
    Test overriding the log level using an environment variable.
    """
    # Set the environment variable to override the log level
    env: dict = {"PYDOUGH_LOG_LEVEL": "DEBUG"}
    code = r"""
from pydough.logger import get_logger

logger = get_logger("env_test_logger")
logger.debug("This is a DEBUG message")
"""
    p = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True
    )
    assert "[DEBUG]" in p.stdout
    assert "This is a DEBUG message" in p.stdout


def test_get_logger_custom_handler(monkeypatch):
    """
    Test logger with a custom handler.
    """
    # Create a custom handler
    buf = StringIO()
    custom_handler = logging.StreamHandler(buf)
    custom_handler.setLevel(logging.ERROR)
    monkeypatch.setenv("PYDOUGH_LOG_LEVEL", "ERROR")
    # Create the logger with the custom handler
    logger = get_logger("custom_handler_test_logger", handlers=[custom_handler])
    # Avoid interference from root handlers
    logger.propagate = False
    # Log messages of different levels
    logger.warning("This is a WARNING message")
    logger.error("This is an ERROR message")

    custom_handler.flush()
    output = buf.getvalue()
    assert (
        "WARNING" not in output
    )  # WARNING is below the custom handler's level (ERROR)
    assert "ERROR" in output
    assert "This is an ERROR message" in output


def test_get_logger_no_duplicate_handlers():
    """
    Test that duplicate handlers are not added to the logger.
    """
    logger = get_logger("no_duplicate_test_logger")
    # Store the initial number of handlers
    initial_handler_count = len(logger.handlers)
    # Call `get_logger` again with the same name
    logger = get_logger("no_duplicate_test_logger")
    assert len(logger.handlers) == initial_handler_count


def test_get_logger_format():
    """
    Test if the log format is correctly applied.
    """
    code = r"""
from pydough.logger import get_logger

log_format = "%(levelname)s - %(message)s"
logger = get_logger("format_test_logger", fmt=log_format)
logger.error("Test format")
"""
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert "ERROR - Test format" in p.stdout


@pytest.mark.skip(reason="Requires design discussion")
def test_get_logger_env_level_overriding_default_value():
    """
    Test if the logger picks up level from env variable when given default_value in the arguments
    """
    # Set the environment variable to override the log level
    env: dict = {"PYDOUGH_LOG_LEVEL": "DEBUG"}
    code = r"""
from pydough.logger import get_logger
import logging

logger = get_logger(
    name="env_level_overriding_default_value_test_logger",
    default_level=logging.INFO,
)
logger.debug("This is a DEBUG message")
"""
    p = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True
    )
    assert "[DEBUG]" in p.stdout
    assert "This is a DEBUG message" in p.stdout


@pytest.mark.parametrize(
    "level_str, expected_level",
    [
        ("DEBUG", logging.DEBUG),
        ("INFO", logging.INFO),
        ("WARNING", logging.WARNING),
        ("ERROR", logging.ERROR),
        ("CRITICAL", logging.CRITICAL),
    ],
)
def test_get_logger_with_env_variable(level_str, expected_level, monkeypatch):
    """
    Test the logger configuration when the `PYDOUGH_LOG_LEVEL` environment variable is set.
    """
    monkeypatch.setenv("PYDOUGH_LOG_LEVEL", level_str)
    logger = get_logger(name=f"get_logger_with_env_variable_test_logger_{level_str}")

    # Assert that the logger level matches the expected level
    assert logger.level == expected_level, (
        f"Expected level {expected_level}, but got {logger.level}"
    )


@pytest.mark.parametrize(
    "expected_level",
    [
        (logging.DEBUG),
        (logging.INFO),
        (logging.WARNING),
        (logging.ERROR),
        (logging.CRITICAL),
    ],
)
def test_get_logger_with_default_level_variable(expected_level):
    """
    Test the logger configuration when the default_level is set.
    """
    logger = get_logger(
        name=f"get_logger_with_default_level_variable_test_logger_{expected_level}",
        default_level=expected_level,
    )

    # Assert that the logger level matches the expected level
    assert logger.level == expected_level, (
        f"Expected level {expected_level}, but got {logger.level}"
    )


def test_get_logger_invalid_env_level(monkeypatch):
    """
    Test the logger when an invalid `PYDOUGH_LOG_LEVEL` environment variable is set.
    """
    # Set an invalid level
    monkeypatch.setenv("PYDOUGH_LOG_LEVEL", "INVALID")

    with pytest.raises(AssertionError):
        get_logger(name="logger_invalid_env_level_test_logger")


def test_execute_df_logging(sqlite_tpch_session: PyDoughSession) -> None:
    """
    Test the example TPC-H relational trees executed on a
    SQLite database, and capture log output printed
    """
    root = tpch_query_1_plan()

    # Create a custom handler
    buf = StringIO()
    custom_handler = logging.StreamHandler(buf)
    # Create the logger with the custom handler
    logger = get_logger(handlers=[custom_handler])
    # Avoid interference from root handlers
    logger.propagate = False

    execute_df(root, sqlite_tpch_session, display_sql=True)
    custom_handler.flush()
    captured_output = buf.getvalue()

    required_op = """
[INFO] pydough.sqlglot.execute_relational: SQL query:
 SELECT
  l_returnflag AS L_RETURNFLAG,
  l_linestatus AS L_LINESTATUS,
  SUM(l_quantity) AS SUM_QTY,
  SUM(l_extendedprice) AS SUM_BASE_PRICE,
  SUM((
    l_extendedprice * (
      1 - l_discount
    )
  )) AS SUM_DISC_PRICE,
  SUM(l_extendedprice * (
    1 - l_discount
  ) * (
    1 + l_tax
  )) AS SUM_CHARGE,
  CAST(SUM(l_quantity) AS REAL) / COUNT(*) AS AVG_QTY,
  CAST(SUM(l_extendedprice) AS REAL) / COUNT(*) AS AVG_PRICE,
  CAST(SUM(l_discount) AS REAL) / COUNT(*) AS AVG_DISC,
  COUNT(*) AS COUNT_ORDER
FROM lineitem
WHERE
  l_shipdate <= '1998-12-01'
GROUP BY
  1,
  2
ORDER BY
  1,
  2
"""
    assert required_op.strip() in captured_output.strip(), (
        f"'{required_op.strip()}' not found in captured output: {captured_output.strip()}"
    )
