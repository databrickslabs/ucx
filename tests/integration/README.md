# Selective running of tests per pull request

Only relevant integration tests are run on the pull request. The logic is as follows:

1. We're running `git diff --name-only` and get a list of file names
2. For every child folder in `tests/integration` we check if any of the files in the folder are in the list of changed files
3. If any of the files in the folder are in the list of changed files, we run the tests in that folder
4. If none of the files in the folder are in the list of changed files, we skip the tests in that folder

Filtering does not apply for:
1. Debugging
2. Nightly
