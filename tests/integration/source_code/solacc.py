import logging
import sys
from pathlib import Path

import requests
from databricks.labs.blueprint.logger import install_logger
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.contexts.workspace_cli import LocalCheckoutContext
from databricks.labs.ucx.framework.utils import run_command
from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.base import CurrentSessionState
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


def lint_all():
    # pylint: disable=too-many-nested-blocks
    ws = WorkspaceClient(host='...', token='...')
    ctx = LocalCheckoutContext(ws).replace(linter_context_factory=lambda: LinterContext(MigrationIndex([])))
    parseable = 0
    missing_imports = 0
    session_state = CurrentSessionState()
    all_files = list(dist.glob('**/*.py'))
    for file in all_files:
        try:
            for located_advice in ctx.local_code_linter.lint_path(file, session_state):
                if located_advice.advice.code == 'import-not-found':
                    missing_imports += 1
                message = located_advice.message_relative_to(dist.parent, default=file)
                sys.stdout.write(f"{message}\n")
            parseable += 1
        except Exception as e:  # pylint: disable=broad-except
            # here we're most likely catching astroid & sqlglot errors
            logger.error(f"Error during parsing of {file}: {e}".replace("\n", " "))
    parseable_pct = int(parseable / len(all_files) * 100)
    logger.info(f"Parseable: {parseable_pct}% ({parseable}/{len(all_files)}), missing imports: {missing_imports}")
    if parseable_pct < 100:
        sys.exit(1)


if __name__ == "__main__":
    install_logger()
    logging.root.setLevel(logging.INFO)
    logger.info("Cloning...")
    clone_all()
    logger.info("Linting...")
    lint_all()
