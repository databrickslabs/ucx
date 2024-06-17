import logging
import os
import sys
from pathlib import Path

import requests
from databricks.labs.blueprint.logger import install_logger
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.contexts.workspace_cli import LocalCheckoutContext
from databricks.labs.ucx.framework.utils import run_command
from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
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


def lint_one(file: Path, ctx: LocalCheckoutContext, unparsed: Path | None) -> tuple[int, int]:
    try:
        missing_imports = 0
        for located_advice in ctx.local_code_linter.lint_path(file):
            if located_advice.advice.code == 'import-not-found':
                missing_imports += 1
            message = located_advice.message_relative_to(dist.parent, default=file)
            sys.stdout.write(f"{message}\n")
        return missing_imports, 1
    except Exception as e:  # pylint: disable=broad-except
        # here we're most likely catching astroid & sqlglot errors
        if unparsed is None:  # linting single file, log exception detqils
            logger.error(f"Error during parsing of {file}: {e}".replace("\n", " "), exc_info=e)
        else:
            logger.error(f"Error during parsing of {file}: {e}".replace("\n", " "))
            # populate solacc-unparsed.txt
            with unparsed.open(mode="a", encoding="utf-8") as f:
                f.write(file.relative_to(dist).as_posix())
                f.write("\n")
        return 0, 0


def lint_all(file_to_lint: str | None):
    ws = WorkspaceClient(host='...', token='...')
    ctx = LocalCheckoutContext(ws).replace(
        linter_context_factory=lambda session_state: LinterContext(MigrationIndex([]), session_state)
    )
    parseable = 0
    missing_imports = 0
    all_files = list(dist.glob('**/*.py')) if file_to_lint is None else [Path(dist, file_to_lint)]
    unparsed: Path | None = None
    if file_to_lint is None:
        unparsed = Path(Path(__file__).parent, "solacc-unparsed.txt")
        if unparsed.exists():
            os.remove(unparsed)
    skipped: set[str] | None = None
    malformed = Path(Path(__file__).parent, "solacc-malformed.txt")
    if file_to_lint is None and malformed.exists():
        text = malformed.read_text(encoding="utf-8")
        skipped = set(text.split("\n"))
    for file in all_files:
        if skipped and file.relative_to(dist).as_posix() in skipped:
            continue
        _missing_imports, _parseable = lint_one(file, ctx, unparsed)
        missing_imports += _missing_imports
        parseable += _parseable
    parseable_pct = int(parseable / len(all_files) * 100)
    logger.info(
        f"Skipped: {len(skipped or [])}, parseable: {parseable_pct}% ({parseable}/{len(all_files)}), missing imports: {missing_imports}"
    )
    if parseable_pct < 100:
        sys.exit(1)


def main(args: list[str]):
    install_logger()
    logging.root.setLevel(logging.INFO)
    file_to_lint = args[1] if len(args) > 1 else None
    if not file_to_lint:
        # don't clone if linting just one file, assumption is we're troubleshooting
        logger.info("Cloning...")
        clone_all()
    logger.info("Linting...")
    lint_all(file_to_lint)


if __name__ == "__main__":
    main(sys.argv)
