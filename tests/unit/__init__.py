import logging

logging.getLogger("tests").setLevel("DEBUG")

DEFAULT_CONFIG = {
    "config.yml": {
        'version': 2,
        'inventory_database': 'ucx',
        'connect': {
            'host': 'foo',
            'token': 'bar',
        },
    },
}
