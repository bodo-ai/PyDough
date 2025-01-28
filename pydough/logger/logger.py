import logging
import os
import sys


def get_logger(
    name=__name__, 
    default_level=logging.INFO, 
    fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=None,
    level_env_var="PYDOUGH_LOG_LEVEL"
):
    """
    Returns a logger with specified handlers, allowing the logging level to be set externally.

    :param name: Logger name, usually __name__ from the calling module
    :param default_level: Default logging level if not set externally
    :param fmt: Log message format string
    :param handlers: A list of logging.Handler instances to add to the logger
    :param level_env_var: Environment variable to override the logging level
    :return: Configured logger instance
    """
    logger = logging.getLogger(name)
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
