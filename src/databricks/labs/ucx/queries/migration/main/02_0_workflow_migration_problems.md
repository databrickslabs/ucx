-- widget title=Workflow migration problems, row=2, col=0, size_x=2, size_y=8

## 2 - Workflow migration problems

The table on the right assist with verifying if workflows are Unity Catalog compatible. It can be filtered on the code
path, problem code and workflow name. The table:
- Points to the code with a detected problem using the code path, workflow & task reference and start/end line & column;
- Explains the detected problem with a humanreadable message and a problem code.

The workflow migration problems are updated after running
[workflow linter workflow](https://github.com/databrickslabs/ucx/blob/main/README.md#workflow-linter-workflow).
