import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import requests
from databricks.labs.blueprint.logger import install_logger
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.contexts.workspace_cli import LocalCheckoutContext
from databricks.labs.ucx.framework.utils import run_command
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import LocatedAdvice
from databricks.labs.ucx.source_code.linters.context import LinterContext

logger = logging.getLogger("verify-accelerators")

this_file = Path(__file__)
dist = (this_file / '../../../../dist').resolve().absolute()


def clone_all():
    params = {'per_page': 100, 'page': 1}
    to_clone = []
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
            return
        params['page'] += 1
        for repo in result:
            to_clone.append(repo['clone_url'])
    dist.mkdir(exist_ok=True)
    to_clone = sorted(to_clone)  # [:10]
    for url in to_clone:
        dst = dist / url.split("/")[-1].split(".")[0]
        if dst.exists():
            continue
        logger.info(f'Cloning {url} into {dst}')
        run_command(f'git clone {url} {dst}')


def collect_missing_imports(advices: list[LocatedAdvice]):
    missing_imports: set[str] = set()
    for located_advice in advices:
        if located_advice.advice.code == 'import-not-found':
            missing_imports.add(located_advice.advice.message.split(':')[1].strip())
    return missing_imports


def collect_uninferrable_count(advices: list[LocatedAdvice]):
    not_computed = 0
    for located_advice in advices:
        if "computed" in located_advice.advice.message:
            not_computed += 1
    return not_computed


def print_advices(advices: list[LocatedAdvice], file: Path):
    for located_advice in advices:
        message = located_advice.message_relative_to(dist.parent, default=file)
        sys.stdout.write(f"{message}\n")


@dataclass
class SolaccContext:
    unparsed_path: Path | None = None
    files_to_skip: set[str] | None = None
    total_count = 0
    parseable_count = 0
    uninferrable_count = 0
    missing_imports: dict[str, dict[str, int]] = field(default_factory=dict)

    @classmethod
    def create(cls, lint_all: bool):
        unparsed_path: Path | None = None
        # if lint_all, recreate "solacc-unparsed.txt"
        if lint_all is None:
            unparsed_path = Path(Path(__file__).parent, "solacc-unparsed.txt")
            if unparsed_path.exists():
                os.remove(unparsed_path)
        files_to_skip: set[str] | None = None
        malformed = Path(__file__).parent / "solacc-malformed.txt"
        if lint_all and malformed.exists():
            lines = malformed.read_text(encoding="utf-8").split("\n")
            files_to_skip = set(line for line in lines if len(line) > 0 and not line.startswith("#"))
        return SolaccContext(unparsed_path=unparsed_path, files_to_skip=files_to_skip)

    def register_missing_import(self, missing_import: str):
        prefix = missing_import.split(".")[0]
        details = self.missing_imports.get(prefix, None)
        if details is None:
            details = {}
            self.missing_imports[prefix] = details
        count = details.get(missing_import, 0)
        details[missing_import] = count + 1

    def log_missing_imports(self):
        missing_imports = dict(sorted(self.missing_imports.items(), key=lambda item: sum(item[1].values()), reverse=True))
        for prefix, details in missing_imports.items():
            logger.info(f"Missing import '{prefix}'")
            for item, count in details.items():
                logger.info(f"  {item}: {count} occurrences")



def lint_one(solacc: SolaccContext, file: Path, ctx: LocalCheckoutContext) -> None:
    try:
        advices = list(ctx.local_code_linter.lint_path(file, set()))
        solacc.parseable_count += 1
        missing_imports = collect_missing_imports(advices)
        for missing_import in missing_imports:
            solacc.register_missing_import(missing_import)
        uninferrable_count = collect_uninferrable_count(advices)
        solacc.uninferrable_count += uninferrable_count
        print_advices(advices, file)
    except Exception as e:  # pylint: disable=broad-except
        # here we're most likely catching astroid & sqlglot errors
        # when linting single file, log exception details
        logger.error(f"Error during parsing of {file}: {e}".replace("\n", " "), exc_info=e if solacc.unparsed_path is None else None)
        if solacc.unparsed_path:
            logger.error(f"Error during parsing of {file}: {e}".replace("\n", " "))
            # populate solacc-unparsed.txt
            with solacc.unparsed_path.open(mode="a", encoding="utf-8") as f:
                f.write(file.relative_to(dist).as_posix())
                f.write("\n")


def lint_dir(solacc: SolaccContext, dir: Path, file_to_lint: str | None = None):
    ws = WorkspaceClient(host='...', token='...')
    ctx = LocalCheckoutContext(ws).replace(
        linter_context_factory=lambda session_state: LinterContext(TableMigrationIndex([]), session_state)
    )
    all_files = list(dir.glob('**/*.py')) if file_to_lint is None else [Path(dir, file_to_lint)]
    for file in all_files:
        solacc.total_count += 1
        if solacc.files_to_skip and file.relative_to(dist).as_posix() in solacc.files_to_skip:
            continue
        lint_one(solacc, file, ctx)


def lint_file(file_to_lint: str):
    solacc = SolaccContext.create(False)
    file_path = Path(file_to_lint)
    lint_dir(solacc, file_path.parent, file_path.name)


def lint_all():
    solacc = SolaccContext.create(True)
    for dir in os.listdir(dist):
        lint_dir(solacc, dist / dir)
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
    # fail the job if files are unparseable
    if parseable_pct < 100:
        sys.exit(1)


def main(args: list[str]):
    install_logger()
    logging.root.setLevel(logging.INFO)
    file_to_lint = args[1] if len(args) > 1 else None
    if file_to_lint:
        # don't clone if linting just one file, assumption is we're troubleshooting
        logger.info("Linting...")
        lint_file(file_to_lint)
        return
    logger.info("Cloning...")
    clone_all()
    logger.info("Linting...")
    lint_all()


if __name__ == "__main__":
    main(sys.argv)
