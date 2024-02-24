import json
import pathlib

__dir = pathlib.Path(__file__).parent


def _load_fixture(filename: str):
    with (__dir / filename).open("r") as f:
        return json.load(f)


def get_az_api_mapping(*args, **_):
    mapping = _load_fixture("azure/mappings.json")[0]
    if args[0] in mapping:
        return mapping[args[0]]
    if args[1] in mapping:
        return mapping[args[1]]
    return {}
