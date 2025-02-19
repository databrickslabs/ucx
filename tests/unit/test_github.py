import pytest

from databricks.labs.ucx.github import GITHUB_URL, IssueType, construct_new_issue_url


def test_construct_new_issue_url_starts_with_github_url() -> None:
    """Test that the URL starts with the GitHub URL."""
    url = construct_new_issue_url(IssueType.FEATURE, "title", "body")
    assert url.startswith("https://github.com/databrickslabs/ucx")


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


def test_construct_new_issue_url_makes_url_safe() -> None:
    """Test that the URL is properly URL-encoded."""
    url = construct_new_issue_url(IssueType.FEATURE, "title", "body with spaces")
    assert "body+with+spaces" in url


def test_construct_new_issue_url_advanced() -> None:
    """Test that the URL is properly constructed with advanced parameters."""
    expected = (
        f"https://github.com/databrickslabs/ucx/issues/new"
        "?type=Feature"
        "&title=Autofix+the+following+Python+code"
        "&body=%23+Desired+behaviour%0A%0AAutofix+following+Python+code"
        "%0A%0A%60%60%60+python%0ATODO%3A+Add+relevant+source+code%0A%60%60%60"
        "&labels=migrate%2Fcode%2Cneeds-triage"
    )
    body = "# Desired behaviour\n\nAutofix following Python code\n\n" "``` python\nTODO: Add relevant source code\n```"
    url = construct_new_issue_url(IssueType.FEATURE, "Autofix the following Python code", body, labels={"migrate/code"})
    assert url == expected
