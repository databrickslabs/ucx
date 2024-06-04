import logging
import sys

logger = logging.getLogger(__name__)


def fn():
    logger.info(f'Called fn() from {__name__}')
    return 1


def main():
    logger.info(f'Called main() from {__name__}')
    result = fn()
    logger.info(f'fn() returned {result}')


def install_logger():
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format='%(asctime)s [%(name)s][%(levelname)s] %(message)s',
    )


if __name__ == '__main__':
    install_logger()
    main()
