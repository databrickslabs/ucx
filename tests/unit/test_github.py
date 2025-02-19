from databricks.labs.ucx.github import GITHUB_URL, IssueType, construct_new_issue_url


def test_construct_new_issue_url_starts_with_github_url() -> None:
    url = construct_new_issue_url(IssueType.FEATURE, "title", "body")
    assert url.startswith(GITHUB_URL)
