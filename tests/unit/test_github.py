import pytest

from databricks.labs.ucx.github import GITHUB_URL, IssueType, construct_new_issue_url


def test_construct_new_issue_url_starts_with_github_url() -> None:
    """Test that the URL starts with the GitHub URL."""
    url = construct_new_issue_url(IssueType.FEATURE, "title", "body")
    assert url.startswith(GITHUB_URL)


def test_construct_new_issue_url_with_labels() -> None:
    """Test that the URL contains the labels."""
    url = construct_new_issue_url(IssueType.FEATURE, "title", "body", labels={"label1", "label2"})
    assert "label1" in url
    assert "label2" in url


@pytest.mark.parametrize("labels", [None, {}, {"label1", "label2"}])
def test_construct_new_issue_url_always_has_needs_triage_label(labels: set[str] | None) -> None:
    """Test that the URL always has the `needs-triage` label."""
    url = construct_new_issue_url(IssueType.FEATURE, "title", "body", labels=labels)
    assert "needs-triage" in url
