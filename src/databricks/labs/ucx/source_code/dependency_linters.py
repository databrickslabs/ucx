from collections.abc import Iterable

from databricks.labs.ucx.source_code.base import Advice, Deprecation
from databricks.labs.ucx.source_code.dependencies import AbstractVisitor
from databricks.labs.ucx.source_code.whitelist import UCCompatibility


class ImportChecker(AbstractVisitor):

    def __init__(self):
        self._advices: list[Advice] = []
        self._trace: list[str] = []

    def _build_trace(self):
        return " <- ".join(self._trace)

    def process(self, graph) -> bool | None:
        dependency = graph.dependency
        self._trace.append(dependency.path)
        compatibility = graph.compatibility(dependency)
        if compatibility and compatibility == UCCompatibility.NONE:
            if dependency.source is not None:
                import_type = dependency.source.location.code
                self._advices.append(
                    dependency.source.location.as_deprecation().replace(
                        code="dependency-check",
                        message="Deprecated " + import_type + ": " + self._build_trace(),
                    )
                )

            else:
                self._advices.append(
                    Deprecation(
                        code="dependency-check",
                        message=f"Use of dependency {dependency.path} is deprecated",
                        start_col=0,
                        end_col=0,
                        start_line=0,
                        end_line=0,
                    )
                )
        if graph.dependency_count == 0 and self._trace:
            self._trace.pop()
        # Do not interrupt the visit
        return False

    def get_advices(self) -> Iterable[Advice]:
        yield from self._advices
