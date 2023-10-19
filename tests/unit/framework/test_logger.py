import logging


def test_logger():
    logger = logging.getLogger(__name__)
    logger.debug("This is a debug message")
    logger.info("This is an table message")
    logger.warning("This is a warning message")
    logger.error("This is an error message", exc_info=KeyError(123))
    logger.critical("This is a critical message")
