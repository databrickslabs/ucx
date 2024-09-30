import dataclasses
import json
import logging
import os
import shutil
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import requests
from databricks.labs.blueprint.logger import install_logger
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.contexts.workspace_cli import LocalCheckoutContext
from databricks.labs.ucx.framework.utils import run_command
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import LocatedAdvice
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.path_lookup import PathLookup

logger = logging.getLogger("verify-accelerators")

this_file = Path(__file__)
dist = (this_file / '../../../../dist').resolve().absolute()
build = dist.parent / "build"
build.mkdir(exist_ok=True)


def _get_repos_to_clone() -> dict[str, str]:
    params = {'per_page': 100, 'page': 1}
    to_clone: dict[str, str] = {}
    while True:
        result = requests.get(
            'https://api.github.com/orgs/databricks-industry-solutions/repos',
            params=params,
            timeout=30,
        ).json()
        if not result:
            break
        if 'message' in result:
            logger.error(result['message'])
            return to_clone
        params['page'] += 1
        for repo in result:
            name = repo['name']
            if name == '.github':
                continue
            to_clone[name] = repo['clone_url']
    return to_clone


def _clone_repo(repo_url, repo_name):
    dist.mkdir(exist_ok=True)
    dst = dist / repo_name
    if dst.exists():
        return dst
    logger.info(f'Cloning {repo_url} into {dst}')
    run_command(f'git clone {repo_url} {dst}')
    return dst


def _collect_missing_imports(advices: list[LocatedAdvice]):
    missing_imports: set[str] = set()
    for located_advice in advices:
        if located_advice.advice.code == 'import-not-found':
            missing_imports.add(located_advice.advice.message.split(':')[1].strip())
    return missing_imports


def _collect_uninferrable_count(advices: list[LocatedAdvice]):
    not_computed = 0
    for located_advice in advices:
        if "computed" in located_advice.advice.message:
            not_computed += 1
    return not_computed


def _collect_unparseable(advices: list[LocatedAdvice]):
    return list(located_advice for located_advice in advices if located_advice.advice.code == 'parse-error')


def _print_advices(advices: list[LocatedAdvice]):
    messages = list(
        located_advice.message_relative_to(dist.parent).replace('\n', ' ') + '\n' for located_advice in advices
    )
    if os.getenv("CI"):
        advices_path = build / "advices.txt"
        with advices_path.open("a") as advices_file:
            advices_file.writelines(messages)
    else:
        for message in messages:
            sys.stdout.write(message)


@dataclass
class _SolaccStats:
    run_id: str
    name: str
    start_timestamp: datetime
    end_timestamp: datetime
    files_count: int
    files_size: int


@dataclass
class _SolaccContext:
    unparsed_files_path: Path | None = None
    files_to_skip: set[Path] | None = None
    total_count = 0
    parseable_count = 0
    uninferrable_count = 0
    missing_imports: dict[str, dict[str, int]] = field(default_factory=dict)
    stats: list[_SolaccStats] = field(default_factory=list)

    @classmethod
    def create(cls, for_all_dirs: bool):
        unparsed_path: Path | None = None
        # if lint_all, recreate "solacc-unparsed.txt"
        if for_all_dirs:
            unparsed_path = Path(Path(__file__).parent, "solacc-unparsed.txt")
            if unparsed_path.exists():
                os.remove(unparsed_path)
        files_to_skip: set[Path] | None = None
        malformed = Path(__file__).parent / "solacc-malformed.txt"
        if for_all_dirs and malformed.exists():
            lines = malformed.read_text(encoding="utf-8").split("\n")
            files_to_skip = set(dist / line for line in lines if len(line) > 0 and not line.startswith("#"))
        return _SolaccContext(unparsed_files_path=unparsed_path, files_to_skip=files_to_skip)

    def register_missing_import(self, missing_import: str):
        prefix = missing_import.split(".")[0]
        details = self.missing_imports.get(prefix, None)
        if details is None:
            details = {}
            self.missing_imports[prefix] = details
        count = details.get(missing_import, 0)
        details[missing_import] = count + 1

    def log_missing_imports(self):
        missing_imports = dict(
            sorted(self.missing_imports.items(), key=lambda item: sum(item[1].values()), reverse=True)
        )
        for prefix, details in missing_imports.items():
            logger.info(f"Missing import '{prefix}'")
            for item, count in details.items():
                logger.info(f"  {item}: {count} occurrences")


class _CleanablePathLookup(PathLookup):

    def __init__(self):
        super().__init__(Path.cwd(), [Path(path) for path in sys.path])
        self._original_sys_paths = set(self._sys_paths)

    def clean_tmp_sys_paths(self):
        for path in self._sys_paths:
            if path in self._original_sys_paths:
                continue
            if path.is_file():
                path.unlink()
            if path.is_dir():
                shutil.rmtree(path)


def _lint_dir(solacc: _SolaccContext, soldir: Path):
    path_lookup = _CleanablePathLookup()
    ws = WorkspaceClient(host='...', token='...')
    ctx = LocalCheckoutContext(ws).replace(
        linter_context_factory=lambda session_state: LinterContext(TableMigrationIndex([]), session_state),
        path_lookup=path_lookup,
    )
    all_files = list(soldir.glob('**/*.py')) + list(soldir.glob('**/*.sql'))
    solacc.total_count += len(all_files)
    # pre-populate linted_files such that files to skip are not linted
    files_to_skip = set(solacc.files_to_skip) if solacc.files_to_skip else set()
    linted_files = set(files_to_skip)
    # lint solution
    start_timestamp = datetime.now(timezone.utc)
    advices = list(ctx.local_code_linter.lint_path(soldir, linted_files))
    end_timestamp = datetime.now(timezone.utc)
    # record stats
    stats = _SolaccStats(
        run_id=os.getenv("GITHUB_RUN_ATTEMPT") or "local",
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        name=soldir.name,
        files_count=len(all_files),
        files_size=sum(path.stat().st_size for path in [soldir / filename for filename in all_files]),
    )
    solacc.stats.append(stats)
    # collect unparseable files
    unparseables = _collect_unparseable(advices)
    solacc.parseable_count += len(linted_files) - len(files_to_skip) - len(set(advice.path for advice in unparseables))
    if solacc.unparsed_files_path:
        for unparseable in unparseables:
            logger.error(f"Error during parsing of {unparseable.path}: {unparseable.advice.message}".replace("\n", " "))
            # populate solacc-unparsed.txt
            with solacc.unparsed_files_path.open(mode="a", encoding="utf-8") as f:
                try:
                    path = unparseable.path.relative_to(dist)
                except ValueError:
                    path = unparseable.path
                f.write(path.as_posix())
                f.write("\n")
    # collect missing imports
    for missing_import in _collect_missing_imports(advices):
        solacc.register_missing_import(missing_import)
    # collect uninferrable
    solacc.uninferrable_count += _collect_uninferrable_count(advices)
    # display advices
    _print_advices(advices)
    # cleanup tmp dirs
    path_lookup.clean_tmp_sys_paths()


def _lint_repos(clone_urls, sol_to_lint: str | None):
    solacc = _SolaccContext.create(sol_to_lint is not None)
    if sol_to_lint:
        sol_dir = _clone_repo(clone_urls[sol_to_lint], sol_to_lint)
        _lint_dir(solacc, sol_dir)
    else:
        names: list[str] = list(clone_urls.keys())
        for name in sorted(names, key=str.casefold):
            logger.info(f"Cloning {name}...")
            sol_dir = _clone_repo(clone_urls[name], name)
            logger.info(f"Linting {name}...")
            _lint_dir(solacc, sol_dir)
            if os.getenv("CI"):
                shutil.rmtree(sol_dir)
    all_files_len = solacc.total_count - (len(solacc.files_to_skip) if solacc.files_to_skip else 0)
    parseable_pct = int(solacc.parseable_count / all_files_len * 100)
    missing_imports_count = sum(sum(details.values()) for details in solacc.missing_imports.values())
    logger.info(
        f"Skipped: {len(solacc.files_to_skip or [])}, "
        f"parseable: {parseable_pct}% ({solacc.parseable_count}/{all_files_len}), "
        f"missing imports: {missing_imports_count}, "
        f"not computed: {solacc.uninferrable_count}"
    )
    solacc.log_missing_imports()
    # log stats
    stats_path = build / "stats.json"
    with stats_path.open("a") as stats_file:
        for stats in solacc.stats:
            message = json.dumps(dataclasses.asdict(stats), default=str)
            stats_file.writelines([message])
    # fail the job if files are unparseable
    if parseable_pct < 100:
        sys.exit(1)


def main(args: list[str]):
    install_logger()
    logging.root.setLevel(logging.INFO)
    sol_to_lint = args[1] if len(args) > 1 else None
    logger.info("Fetching repos to clone...")
    repo_urls = _get_repos_to_clone()
    _lint_repos(repo_urls, sol_to_lint)


if __name__ == "__main__":
    main(sys.argv)
