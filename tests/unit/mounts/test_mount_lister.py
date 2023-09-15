import logging
from unittest.mock import MagicMock

from databricks.sdk.service.compute import (
    CommandStatusResponse,
    InstanceProfile,
    Results,
)

from databricks.labs.ucx.mounts.list_mounts import MountLister, MountResult

logger = logging.getLogger(__name__)


def test_two_mounts_with_one_instance_profile_should_return_two_mount_results():
    client = MagicMock()
    data = """{"mount_1":"s3a://bucket_1/files_to_read","mount_2":"s3a://bucket_2/test_path"}
        warning: one feature warning; for details, enable `:setting -feature' or `:replay -feature'
        import scala.concurrent._
        import scala.concurrent.duration._
    """
    client.command_execution.execute.return_value.result.return_value = CommandStatusResponse(
        id="1", results=Results(data=data)
    )

    lister = MountLister(client)
    result = lister.list_mounts([InstanceProfile("arn:aws:iam::000000000000:instance-profile/ucx-unit-test")])

    assert len(result) == 2
    assert result == [
        MountResult(
            mount_source="mount_1",
            mount_path="s3a://bucket_1/files_to_read",
            aws_instance_profile_arn="arn:aws:iam::000000000000:instance-profile/ucx-unit-test",
            aws_iam_role_arn="",
            aws_is_meta_instance_profile="",
        ),
        MountResult(
            mount_source="mount_2",
            mount_path="s3a://bucket_2/test_path",
            aws_instance_profile_arn="arn:aws:iam::000000000000:instance-profile/ucx-unit-test",
            aws_iam_role_arn="",
            aws_is_meta_instance_profile="",
        ),
    ]


# TODO: Test fails because side effect method isn't applied
# def test_two_mounts_with_two_instance_profile_should_return_multiple_mount_results():
#    client = MagicMock()
#
#    def cluster_side_effect(*args, **kwargs):
#        if args[0] == "ucx-unit-test":
#            return ClusterDetails(cluster_id="1")
#        elif args[0] == "ucx-unit-test-2":
#            return ClusterDetails(cluster_id="2")
#
#    # TODO: Make this test pass by applying the side effect method properly
#    client.clusters.create.return_value.result.side_effect = cluster_side_effect
#
#    def command_exec_side_effect(cluster_id, **kwargs):
#        if cluster_id == 1:
#            data = """{"mount_1":"s3a://bucket_1/files_to_read","mount_2":"s3a://bucket_2/test_path"}"""
#            return CommandStatusResponse(id="1", results=Results(data=data))
#        elif cluster_id == 2:
#            data = """{"mount_1":"s3a://bucket_1/files_to_read","mount_3":"s3a://bucket_3/path"}"""
#            return CommandStatusResponse(id="2", results=Results(data=data))
#
#    client.command_execution.execute.return_value.result.side_effect = command_exec_side_effect
#
#    lister = MountLister(client)
#    result = lister.list_mounts(
#        [
#            InstanceProfile("arn:aws:iam::000000000000:instance-profile/unit-test"),
#            InstanceProfile("arn:aws:iam::000000000000:instance-profile/unit-test-2"),
#        ]
#    )
#
#    assert len(result) == 4
#    assert result == [
#        MountResult(
#            mount_source="mount_1",
#            mount_path="s3a://bucket_1/files_to_read",
#            aws_instance_profile_arn="arn:aws:iam::000000000000:instance-profile/ucx-unit-test",
#            aws_iam_role_arn="",
#            aws_is_meta_instance_profile="",
#        ),
#        MountResult(
#            mount_source="mount_2",
#            mount_path="s3a://bucket_2/test_path",
#            aws_instance_profile_arn="arn:aws:iam::000000000000:instance-profile/ucx-unit-test",
#            aws_iam_role_arn="",
#            aws_is_meta_instance_profile="",
#        ),
#    ]
