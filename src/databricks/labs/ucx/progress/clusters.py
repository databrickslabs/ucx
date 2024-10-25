from databricks.labs.ucx.assessment.clusters import PolicyInfo
from databricks.labs.ucx.progress.history import ProgressEncoder


class ClusterPolicyProgressEncoder(ProgressEncoder[PolicyInfo]):
    """Encode class:PolicyInfo to class:History.

    The PolicyInfo failures are added during crawling. For legacy reasons, we keep this logic in the crawler`. This
    class is a placeholder for documenting this decision and maintaining a parallel structure across progress modules.
    """
