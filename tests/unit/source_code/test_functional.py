from __future__ import annotations

import json
import re
import tokenize
from collections.abc import Iterable, Generator

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex, MigrationStatus
from databricks.labs.ucx.source_code.base import Advice, CurrentSessionState
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.sources import FileLinter
from databricks.labs.ucx.source_code.path_lookup import PathLookup


@dataclass(frozen=True, kw_only=True, slots=True)
class Comment:
    text: str
    # Line positions are 0-based.
    start_line: int
    end_line: int

    @classmethod
    def from_token(cls, token: tokenize.TokenInfo) -> Comment:
        # Python's tokenizer uses 1-based line numbers.
        return cls(text=token.string, start_line=token.start[0] - 1, end_line=token.end[0] - 1)


@dataclass(frozen=True, kw_only=True, slots=True, order=True)
class Expectation:
    # Field order is used for natural ordering/sorting.
    # Line and column positions are both 0-based.
    start_line: int
    start_col: int
    end_line: int
    end_col: int

    code: str
    message: str

    def __str__(self):
        return f"# ucx[{self.code}:{self.start_line}:{self.start_col}:{self.end_line}:{self.end_col}] {self.message}"

    @classmethod
    def from_advice(cls, advice: Advice) -> Expectation:
        """Convenience conversion factory."""
        return cls(
            code=advice.code,
            message=advice.message,
            start_line=advice.start_line,
            start_col=advice.start_col,
            end_col=advice.end_col,
            end_line=advice.end_line,
        )


class Functional:
    _re = re.compile(
        r"# ucx\[(?P<code>[\w-]+):(?P<start_line>[\d+]+):(?P<start_col>[\d]+):(?P<end_line>[\d+]+):(?P<end_col>[\d]+)] (?P<message>.*)"
    )
    _re_session_state = re.compile(r'# ucx\[session-state] (?P<session_state_json>\{.*})')

    _location = Path(__file__).parent / 'samples/functional'

    @classmethod
    def for_child(cls, child: str, parents: list[str]) -> Functional:
        child_path = cls._location / child
        parent_paths = [ cls._location / parent for parent in parents ]
        return Functional(child_path, parent_paths)

    @classmethod
    def all(cls) -> list[Functional]:
        return [Functional(path) for path in cls._location.glob('**/*.py') if not path.name.startswith("_")]

    @classmethod
    def test_id(cls, sample: Functional) -> str:
        return sample.path.relative_to(cls._location).as_posix()

    def __init__(self, path: Path, parents: list[Path] | None = None) -> None:
        self.path = path
        self.parents = parents

    def verify(self, path_lookup: PathLookup) -> None:
        expected_problems = list(self._expected_problems())
        actual_advices = list(self._lint(path_lookup))
        # Convert the actual problems to the same type as our expected problems for easier comparison.
        actual_problems = [Expectation.from_advice(advice) for advice in actual_advices]

        # Fail the test if the comments don't match reality.
        expected_but_missing = sorted(set(expected_problems).difference(actual_problems))
        unexpected = sorted(set(actual_problems).difference(expected_problems))
        errors = []
        if expected_but_missing:
            errors.append("Expected linting advice that didn't occur:")
            errors += [f"  {advice}" for advice in expected_but_missing]
        if unexpected:
            errors.append("Unexpected linting advice encountered:")
            errors += [f"  {advice}" for advice in unexpected]
        if errors:
            errors.insert(0, f"Functional sample: {self.path.relative_to(Path(__file__).parent)}")
        no_errors = not errors
        assert no_errors, "\n".join(errors)
        # TODO: output annotated file with comments for quick fixing

    def _lint(self, path_lookup: PathLookup) -> Iterable[Advice]:
        migration_index = MigrationIndex(
            [
                MigrationStatus('old', 'things', dst_catalog='brand', dst_schema='new', dst_table='stuff'),
                MigrationStatus('other', 'matters', dst_catalog='some', dst_schema='certain', dst_table='issues'),
            ]
        )
        session_state = self._test_session_state()
        print(str(session_state))
        session_state.named_parameters = {"my-widget": "my-path.py"}
        ctx = LinterContext(migration_index, session_state)
        linter = FileLinter(ctx, path_lookup, session_state, self.path)
        return linter.lint()

    def _regex_match(self, regex: re.Pattern[str]) -> Generator[tuple[Comment, dict[str, Any]], None, None]:
        with self.path.open('rb') as f:
            for comment in self._comments(f):
                if not comment.text.startswith('# ucx['):
                    continue
                match = regex.match(comment.text)
                if not match:
                    continue
                groups = match.groupdict()
                yield comment, groups

    def _expected_problems(self) -> Generator[Expectation, None, None]:
        for comment, groups in self._regex_match(self._re):
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

    def _test_session_state(self) -> CurrentSessionState:
        matches = list(self._regex_match(self._re_session_state))
        if len(matches) > 1:
            raise ValueError("A test should have no more than one session state definition")
        if len(matches) == 0:
            return CurrentSessionState()
        groups = matches[0][1]
        json_str = groups['session_state_json']
        return CurrentSessionState.from_json(json.loads(json_str))

    @staticmethod
    def _comments(f) -> Generator[Comment, None, None]:
        for token in tokenize.tokenize(f.readline):
            if token.type != tokenize.COMMENT:
                continue
            yield Comment.from_token(token)


@pytest.mark.parametrize("sample", Functional.all(), ids=Functional.test_id)
def test_functional(sample: Functional, mock_path_lookup) -> None:
    path_lookup = mock_path_lookup.change_directory(sample.path.parent)
    sample.verify(path_lookup)


@pytest.mark.parametrize(
    "child, parents", [("_child_uses_value_from_parent.py", ["parent_runs_child_that_uses_value_from_parent.py"])]
)
def test_functional_with_parent(child: str, parents: list[str], mock_path_lookup) -> None:
    sample = Functional.for_child(child, parents)
    path_lookup = mock_path_lookup.change_directory(sample.path.parent)
    sample.verify(path_lookup)


@pytest.mark.skip(reason="Used for troubleshooting failing tests")
def test_one_functional(mock_path_lookup):
    path = mock_path_lookup.resolve(Path("functional/values_across_notebooks_magic_line.py"))
    path_lookup = mock_path_lookup.change_directory(path.parent)
    sample = Functional(path)
    sample.verify(path_lookup)
