from databricks.labs.ucx.assessment.pipelines import PipelineInfo
from databricks.labs.ucx.progress.history import ProgressEncoder


class PipelineProgressEncoder(ProgressEncoder[PipelineInfo]):
    """Encode class:PipelineInfo to class:History.

    The PipelineInfo failures are added during crawling. For legacy reasons, we keep this logic in the crawler. This
    class is a placeholder for documenting this decision and maintaining a parallel structure across progress modules.
    """
