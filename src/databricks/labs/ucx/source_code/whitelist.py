import csv
import sys
from collections.abc import Iterable


class Whitelist:
    @classmethod
    def parse(cls, data: str):
        fixed_widths = [68, 9] # name and ML-only
        default_width = 18 # versions
        def parse_line(line: str) -> list[str]:
            cells: list[str] = []
            for i in range(0, 1000):
                count = fixed_widths[i] if i < len(fixed_widths) else default_width
                cells.append(line[0:count])
                line = line[count:]
                if len(line) == 0:
                    return cells
            raise NotImplementedError()

        lines = data.split("\n")
        # skip header // header = parse_line(lines[0])
        records = [ parse_line(lines[i]) for i in range(1, len(lines))]
        names = [ record[0].strip() for record in records ]
        return Whitelist(names)

    def __init__(self, names: Iterable[str]):
        self._names = set(names).union(sys.stdlib_module_names)

    def __contains__(self, name: str):
        return name in self._names

    def has(self, name: str):
        return name in self