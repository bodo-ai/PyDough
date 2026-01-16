"""
Configures and returns a logger.
"""

import logging
import os
import sys


def get_logger(
    logger_name: str = "pydough",
    default_level: int = logging.INFO,
    fmt="%(asctime)s [%(levelname)s] %(module)s: %(message)s",
    handlers: list[logging.Handler] | None = None,
) -> logging.Logger:
    """
    Returns a logger with specified handlers, allowing the logging level to be set externally via an environment variable `PYDOUGH_LOG_LEVEL`.
    The default handler redirects to standard output. Additional handlers can be sent as a list.

    Args:
        `logger_name` : The logger name you want to get or create (in case it does not exists)
        `default_level` : Default logging level if not set externally.
        `fmt` : The format of the string compatible with python's logging library.
        `handlers` : A list of `logging.Handler` instances to add to the logger.
    Returns:
        `logging.Logger` : Configured logger instance.
    """
    logger: logging.Logger = logging.getLogger(logger_name)
    level_env: str | None = os.getenv("PYDOUGH_LOG_LEVEL")
    level: int

    if level_env is not None:
        assert isinstance(level_env, str), (
            f"expected environment variable 'PYDOUGH_LOG_LEVEL' to be a string, found {level_env.__class__.__name__}"
        )
        allowed_levels: list[str] = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        assert level_env in allowed_levels, (
            f"expected environment variable 'PYDOUGH_LOG_LEVEL' to be one of {', '.join(allowed_levels)}, found {level_env}"
        )
        # Convert string level (e.g., "DEBUG", "INFO") to a logging constant
        level = getattr(logging, level_env.upper(), default_level)
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
        level = default_level

    # Avoid adding duplicate handlers
    if not logger.handlers:
        # Create default console handler
        default_handler: logging.StreamHandler = logging.StreamHandler(sys.stdout)
        default_handler.setLevel(level)
        # Create formatter
        formatter: logging.Formatter = logging.Formatter(fmt)
        # Attach formatter to the default handler
        default_handler.setFormatter(formatter)
        logger.addHandler(default_handler)
        # Add additional provided handlers
        if handlers:
            for handler in handlers:
                if (handler.level == logging.NOTSET):
                    handler.setLevel(level)
                handler.setFormatter(formatter)
                logger.addHandler(handler)
    logger.setLevel(level)
    return logger
