import json
import sys
from pathlib import Path

import yaml

from databricks.labs.ucx.runtime import Workflows

if __name__ == '__main__':
    workflows = Workflows.all()
    root = Path(__file__).parent / '../..'
    metadata = yaml.safe_load((root / 'labs.yml').read_text())
    keys = workflows._workflows.keys()  # pylint: disable=protected-access
    cmd_names = sorted(set([x["name"] for x in metadata["commands"]] + list(keys)))
    (root / '.venv/cmds.json').write_text(json.dumps(cmd_names, indent=2))
    sys.stderr.write(f'Wrote {len(cmd_names)} command names to {root}/.venv/cmds.json\n')
