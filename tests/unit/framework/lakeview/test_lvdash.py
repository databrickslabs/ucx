import json

from databricks.labs.ucx.framework.lakeview.model import Dashboard


def test_lvdash():
    with open('/Users/serge.smertin/Downloads/Databricks Labs GitHub telemetry.lvdash (1).json') as f:
        raw = json.load(f)
        lvdash = Dashboard.from_dict(raw)
        assert lvdash is not None