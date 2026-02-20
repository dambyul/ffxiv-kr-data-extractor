import logging
import os
import sys

def setup_logging(name="ffxiv_extractor", log_file=None, level=logging.INFO):
    """
    Sets up logging with console and optional file output.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Check if handlers already exist to avoid duplicates
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console Handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File Handler
    if log_file:
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger

def get_logger(name="ffxiv_extractor"):
    return logging.getLogger(name)
