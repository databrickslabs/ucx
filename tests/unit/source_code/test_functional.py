import re
import tokenize
from collections.abc import Iterable, Generator

from dataclasses import dataclass
from pathlib import Path

import pytest

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex, MigrationStatus
from databricks.labs.ucx.source_code.base import Advice
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.sources import FileLinter


@dataclass
class Comment:
    text: str
    start_line: int
    end_line: int


@dataclass
class Expectation:
    code: str
    message: str
    start_line: int
    start_col: int
    end_line: int
    end_col: int


class Functional:
    _re = re.compile(
        r"# ucx\[(?P<code>[\w-]+):(?P<start_line>[\d+]+):(?P<start_col>[\d]+):(?P<end_line>[\d+]+):(?P<end_col>[\d]+)] (?P<message>.*)"
    )
    _location = Path(__file__).parent / 'samples/functional'

    @classmethod
    def all(cls) -> list['Functional']:
        return [Functional(_) for _ in cls._location.glob('**/*.py')]

    @classmethod
    def test_id(cls, sample: 'Functional') -> str:
        return sample.path.relative_to(cls._location).as_posix()

    def __init__(self, path: Path) -> None:
        self.path = path

    def verify(self) -> None:
        expected_problems = list(self._expected_problems())
        actual_problems = sorted(list(self._lint()), key=lambda a: (a.start_line, a.start_col))
        high_level_expected = [f'{p.code}:{p.message}' for p in expected_problems]
        high_level_actual = [f'{p.code}:{p.message}' for p in actual_problems]
        assert high_level_actual == high_level_expected
        # TODO: match start/end lines/columns as well. At the moment notebook parsing has a bug that makes it impossible
        # TODO: output annotated file with comments for quick fixing

    def _lint(self) -> Iterable[Advice]:
        migration_index = MigrationIndex(
            [
                MigrationStatus('old', 'things', dst_catalog='brand', dst_schema='new', dst_table='stuff'),
                MigrationStatus('other', 'matters', dst_catalog='some', dst_schema='certain', dst_table='issues'),
            ]
        )
        ctx = LinterContext(migration_index)
        linter = FileLinter(ctx, self.path)
        return linter.lint()

    def _expected_problems(self) -> Generator[Expectation, None, None]:
        with self.path.open('rb') as f:
            for comment in self._comments(f):
                if not comment.text.startswith('# ucx['):
                    continue
                match = self._re.match(comment.text)
                if not match:
                    continue
                groups = match.groupdict()
                reported_start_line = groups['start_line']
                if '+' in reported_start_line:
                    start_line = int(reported_start_line[1:]) + comment.start_line
                else:
                    start_line = int(reported_start_line)
                reported_end_line = groups['end_line']
                if '+' in reported_end_line:
                    end_line = int(reported_end_line[1:]) + comment.start_line
                else:
                    end_line = int(reported_end_line)
                yield Expectation(
                    code=groups['code'],
                    message=groups['message'],
                    start_line=start_line,
                    start_col=int(groups['start_col']),
                    end_line=end_line,
                    end_col=int(groups['end_col']),
                )

    @staticmethod
    def _comments(f) -> Generator[Comment, None, None]:
        for token in tokenize.tokenize(f.readline):
            if token.type != tokenize.COMMENT:
                continue
            yield Comment(token.string, token.start[0], token.end[0])


@pytest.mark.parametrize("sample", Functional.all(), ids=Functional.test_id)
def test_functional(sample: Functional) -> None:
    sample.verify()
