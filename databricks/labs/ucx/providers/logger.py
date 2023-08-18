from loguru import logger as _loguru_logger

# reassigning the logger to the loguru logger
# for flexibility and simple dependency injection
logger = _loguru_logger
