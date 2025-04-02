import io
import logging
from contextlib import redirect_stdout

import pytest
from tpch_relational_plans import (
    tpch_query_1_plan,
)

from pydough.configs import PyDoughConfigs
from pydough.database_connectors import DatabaseContext
from pydough.logger import get_logger
from pydough.sqlglot import execute_df

pytestmark = [pytest.mark.execute]


def test_get_logger_default_level(capsys):
    """
    Test logger with the default logging level and output.
    """
    logger = get_logger("default_level_test_logger")
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
    logger = get_logger("env_test_logger")
    logger.debug("This is a DEBUG message")
    captured = capsys.readouterr()
    assert "DEBUG" in captured.out
    assert "This is a DEBUG message" in captured.out


def test_get_logger_custom_handler(monkeypatch, capsys):
    """
    Test logger with a custom handler.
    """
    # Create a custom handler
    custom_handler = logging.StreamHandler()
    custom_handler.setLevel(logging.ERROR)
    monkeypatch.setenv("PYDOUGH_LOG_LEVEL", "ERROR")
    # Create the logger with the custom handler
    logger = get_logger("custom_handler_test_logger", handlers=[custom_handler])
    # Log messages of different levels
    logger.warning("This is a WARNING message")
    logger.error("This is an ERROR message")
    captured = capsys.readouterr()
    assert (
        "WARNING" not in captured.out
    )  # WARNING is below the custom handler's level (ERROR)
    assert "ERROR" in captured.out
    assert "This is an ERROR message" in captured.out


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


def test_get_logger_format(capsys):
    """
    Test if the log format is correctly applied.
    """
    log_format = "%(levelname)s - %(message)s"
    logger = get_logger("format_test_logger", fmt=log_format)
    logger.error("Test format")
    captured = capsys.readouterr()
    assert "ERROR - Test format" in captured.out


def test_get_logger_env_level_overriding_default_value(monkeypatch, capsys):
    """
    Test if the logger picks up level from env variable when given default_value in the arguments
    """
    # Set the environment variable to override the log level
    monkeypatch.setenv("PYDOUGH_LOG_LEVEL", "DEBUG")
    logger = get_logger(
        name="env_level_overriding_default_value_test_logger",
        default_level=logging.INFO,
    )
    logger.debug("This is a DEBUG message")
    captured = capsys.readouterr()

    assert "DEBUG" in captured.out
    assert "This is a DEBUG message" in captured.out


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
    logger = get_logger(name="get_logger_with_env_variable_test_logger")

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
        name="get_logger_with_default_level_variable_test_logger",
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


def test_execute_df_logging(
    sqlite_tpch_db_context: DatabaseContext,
    default_config: PyDoughConfigs,
) -> None:
    """
    Test the example TPC-H relational trees executed on a
    SQLite database, and capture any SQL or output printed to stdout.
    """
    root = tpch_query_1_plan()
    # Create a StringIO buffer
    output_capture = io.StringIO()
    # Redirect stdout to the buffer
    with redirect_stdout(output_capture):
        execute_df(
            root,
            sqlite_tpch_db_context,
            default_config,
            display_sql=True,
        )
    # Retrieve the output from the buffer
    captured_output = output_capture.getvalue()
    required_op = """
[INFO] pydough.sqlglot.execute_relational: SQL query:
 WITH "_t0" AS (
  SELECT
    COUNT() AS "count_order",
    SUM("lineitem"."l_discount") AS "sum_discount",
    SUM("lineitem"."l_extendedprice") AS "sum_base_price",
    SUM("lineitem"."l_quantity") AS "sum_qty",
    SUM((
      "lineitem"."l_extendedprice" * (
        1 - "lineitem"."l_discount"
      )
    )) AS "sum_disc_price",
    SUM(
      "lineitem"."l_extendedprice" * (
        1 - "lineitem"."l_discount"
      ) * (
        1 + "lineitem"."l_tax"
      )
    ) AS "sum_charge",
    "lineitem"."l_linestatus" AS "l_linestatus",
    "lineitem"."l_returnflag" AS "l_returnflag"
  FROM "lineitem" AS "lineitem"
  WHERE
    "lineitem"."l_shipdate" <= '1998-12-01'
  GROUP BY
    "lineitem"."l_linestatus",
    "lineitem"."l_returnflag"
)
SELECT
  "_t0"."l_returnflag" AS "L_RETURNFLAG",
  "_t0"."l_linestatus" AS "L_LINESTATUS",
  "_t0"."sum_qty" AS "SUM_QTY",
  "_t0"."sum_base_price" AS "SUM_BASE_PRICE",
  "_t0"."sum_disc_price" AS "SUM_DISC_PRICE",
  "_t0"."sum_charge" AS "SUM_CHARGE",
  CAST("_t0"."sum_qty" AS REAL) / "_t0"."count_order" AS "AVG_QTY",
  CAST("_t0"."sum_base_price" AS REAL) / "_t0"."count_order" AS "AVG_PRICE",
  CAST("_t0"."sum_discount" AS REAL) / "_t0"."count_order" AS "AVG_DISC",
  "_t0"."count_order" AS "COUNT_ORDER"
FROM "_t0" AS "_t0"
ORDER BY
  "l_returnflag",
  "l_linestatus"
"""
    assert required_op.strip() in captured_output.strip(), (
        f"'{required_op.strip()}' not found in captured output: {captured_output.strip()}"
    )
