from databricks.labs.ucx.mounts.list_mounts import MountLister

import logging

logger = logging.getLogger(__name__)

def test_mount_list(ws):
    #TODO: Add proper testing
    lister = MountLister(ws)
    instances = [x for x in ws.instance_profiles.list() if
                 "arn:aws:iam::997819012307:instance-profile/one-env-databricks-access" in x.instance_profile_arn]
    res = lister.list_mounts(instances)
    print(res)
    assert len(res) > 0