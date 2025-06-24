"""
Logging initializer
"""

import logging
import sys

def init_logging(log_level: str):
    """
    Inits the logging module with the given log level.
    """
    log_level_str = log_level.lower()
    if log_level_str == "debug":
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    # This is required to mute the pika logging
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=log_level,  stream=sys.stdout, format="[%(levelname)s] %(message)s")
