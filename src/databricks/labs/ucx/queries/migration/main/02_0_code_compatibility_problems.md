-- widget title=Workflow migration problems, row=2, col=0, size_x=2, size_y=8

## 2 - Code compatibility problems

The table on the right assist with verifying if workflows are Unity Catalog compatible. It can be filtered on the path,
problem code and workflow name. The table:
- Points to a problem detected in the code using the code path, workflow & task reference and start/end line & column;
- Explains the problem with a human-readable message and a code.

The code compatibility problems are updated after running
[Jobs Static Code Analysis Workflow](https://github.com/databrickslabs/ucx/blob/main/README.md#workflow-linter-workflow).
