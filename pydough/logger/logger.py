"""
Module of PyDough dealing with logging across the library
"""

import logging
import os
import sys


def get_logger(
    name: str = __name__,
    default_level: int = logging.INFO,
    fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers: list[logging.Handler] | None = None,
) -> logging.Logger:
    """
    Returns a logger with specified handlers, allowing the logging level to be set externally.
    The default handler redirects to standard output.

    Args:
        name (str): Logger name, usually the `__name__` from the calling module.
        default_level (int): Default logging level if not set externally.
        fmt (str): The format of the string compatible with python's logging library.
        handlers (Optional[List[logging.Handler]]): A list of `logging.Handler` instances to add to the logger.
    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    level_env_var = "PYDOUGH_LOG_LEVEL"
    level = os.getenv(level_env_var, default_level)
    if isinstance(level, str):
        # Convert string level (e.g., "DEBUG", "INFO") to a logging constant
        level = getattr(logging, level.upper(), default_level)
    # Create default console handler
    default_handler = logging.StreamHandler(sys.stdout)
    default_handler.setLevel(level)
    # Create formatter
    formatter = logging.Formatter(fmt)
    # Attach formatter to the default handler
    default_handler.setFormatter(formatter)
    # Avoid adding duplicate handlers
    if not logger.handlers:
        logger.addHandler(default_handler)
        if handlers:
            for handler in handlers:
                handler.setFormatter(formatter)
                logger.addHandler(handler)
    logger.setLevel(level)
    return logger
