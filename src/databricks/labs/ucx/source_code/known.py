from __future__ import annotations

import collections
import json
import logging
import sys
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)


class UCCompatibility(Enum):
    UNKNOWN = "unknown"
    NONE = "none"
    PARTIAL = "partial"
    FULL = "full"

    @classmethod
    def value_of(cls, value: str):
        return next((ucc for ucc in cls if ucc.value == value))


class Whitelist:
    def __init__(self):
        self._module_problems = collections.OrderedDict()
        self._module_distributions = {}
        known_json = Path(__file__).parent / "known.json"
        with known_json.open() as f:
            known = json.load(f)
        for distribution_name, modules in known.items():
            specific_modules_first = sorted(modules.items(), key=lambda x: x[0], reverse=True)
            for module_ref, problems in specific_modules_first:
                self._module_problems[module_ref] = problems
                self._module_distributions[module_ref] = distribution_name
        for name in sys.stdlib_module_names:
            self._module_problems[name] = []
            self._module_distributions[name] = "python"

    def compatibility(self, name: str) -> UCCompatibility:
        if not name:
            return UCCompatibility.UNKNOWN
        for module, problems in self._module_problems.items():
            if not name.startswith(module):
                continue
            if problems:
                return UCCompatibility.PARTIAL
            return UCCompatibility.FULL
        return UCCompatibility.NONE

    def distribution_compatibility(self, name: str) -> UCCompatibility:
        if not name:
            return UCCompatibility.UNKNOWN
        for module, distribution_name in self._module_distributions.items():
            if distribution_name != name:
                continue
            problems = self._module_problems[module]
            if problems:
                return UCCompatibility.PARTIAL
            return UCCompatibility.FULL
        return UCCompatibility.NONE
