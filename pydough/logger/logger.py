"""
Configures and returns a logger.
"""

import logging
import os
import sys


def get_level_source_logger(logger: logging.Logger) -> logging.Logger:
    """
    Returns the logger from which the effective level is inherited.
    """
    current: logging.Logger | None = logger
    while current:
        if current.level != logging.NOTSET:
            return current
        current = current.parent
    return logging.root


def get_logger(
    name: str = "pydough",
    default_level: int | None = None,
    fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers: list[logging.Handler] | None = None,
) -> logging.Logger:
    """
    Returns a logger with specified handlers, allowing the logging level to be set externally via an environment variable `PYDOUGH_LOG_LEVEL`.
    The default handler redirects to standard output. Additional handlers can be sent as a list.

    Args:
        `name` : The logger name you want to get or create (in case it does not exists)
        `default_level` : Default logging level if not set externally.
        `fmt` : The format of the string compatible with python's logging library.
        `handlers` : A list of `logging.Handler` instances to add to the logger.
    Returns:
        `logging.Logger` : Configured logger instance.
    """
    logger: logging.Logger = logging.getLogger(name)
    if default_level is None:
        # Get log level from PYDOUGH_LOG_LEVEL
        level_env = os.getenv("PYDOUGH_LOG_LEVEL")

        if level_env is not None:
            assert isinstance(level_env, str), (
                f"expected environment variable 'PYDOUGH_LOG_LEVEL' to be a string, found {level_env.__class__.__name__}"
            )
            level_env = level_env.upper()
            allowed_levels: list[str] = list(logging._nameToLevel.keys())
            assert level_env in allowed_levels, (
                f"expected environment variable 'PYDOUGH_LOG_LEVEL' to be one of {', '.join(allowed_levels)}, found {default_level}"
            )
            # Convert string level (e.g., "DEBUG", "INFO") to a logging constant
            default_level = getattr(logging, level_env, logging.INFO)
    else:
        assert default_level in [
            logging.DEBUG,
            logging.INFO,
            logging.WARNING,
            logging.ERROR,
            logging.CRITICAL,
        ], (
            f"expected arguement default_value to be one of logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL, found {default_level}"
        )
    level: int = default_level if default_level is not None else logging.INFO

    # Create formatter
    formatter: logging.Formatter = logging.Formatter(fmt)
    if get_level_source_logger(logger).name == "root":
        # Set logLevel to PYDOUGH_LOG_LEVEL if no level has been set for pydough module
        logger.setLevel(level)
    # Add provided handlers
    if handlers:
        for handler in handlers:
            if handler.level == logging.NOTSET:
                handler.setLevel(logger.level)
            if handler.formatter is None:
                handler.setFormatter(formatter)
            logger.addHandler(handler)
    elif not logger.hasHandlers():
        # Create default console handler only if no handlers were provided to avoid adding duplicate handlers
        default_handler: logging.StreamHandler = logging.StreamHandler(sys.stdout)
        default_handler.setLevel(level)
        # Attach formatter to the default handler
        default_handler.setFormatter(formatter)
        logger.addHandler(default_handler)

    return logger
