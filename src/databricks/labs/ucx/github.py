from enum import Enum

DOCS_URL = "https://databrickslabs.github.io/ucx/docs/"
GITHUB_URL = "https://github.com/databrickslabs/ucx"


class IssueType(Enum):
    """The issue type"""

    FEATURE = "Feature"
    BUG = "Bug"
    TASK = "Task"


def construct_new_issue_url(
    issue_type: IssueType,
    title: str,
    body: str,
    *,
    labels: set[str] | None = None,
) -> str:
    """Construct a new issue URL.

    References:
    - https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/creating-an-issue#creating-an-issue-from-a-url-query
    """
    labels = labels or set()
    labels.add("needs-triage")
    return f"{GITHUB_URL}/issues/new?type={issue_type.value}&title={title}&body={body}&labels={','.join(labels)}"
