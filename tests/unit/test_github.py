from databricks.labs.ucx.github import GITHUB_URL, construct_new_issue_url


def test_construct_new_issue_url_starts_with_github_url() -> None:
    assert construct_new_issue_url().startswith(GITHUB_URL)
