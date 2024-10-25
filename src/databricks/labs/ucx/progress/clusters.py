from databricks.labs.ucx.assessment.clusters import ClusterInfo, PolicyInfo
from databricks.labs.ucx.progress.history import ProgressEncoder


class ClusterProgressEncoder(ProgressEncoder[ClusterInfo]):
    """Encode class:ClusterInfo to class:History.

    The ClusterInfo failures are added during crawling. For legacy reasons, we keep this logic in the crawler. This
    class is a placeholder for documenting this decision and maintaining a parallel structure across progress modules.

    The failures associated with a cluster are partially inherited from the cluster policies and init scripts which are
    attached to a cluster.
    """


class ClusterPolicyProgressEncoder(ProgressEncoder[PolicyInfo]):
    """Encode class:PolicyInfo to class:History.

    The PolicyInfo failures are added during crawling. For legacy reasons, we keep this logic in the crawler. This
    class is a placeholder for documenting this decision and maintaining a parallel structure across progress modules.
    """
