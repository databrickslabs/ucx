from astroid import nodes  # type: ignore
from pylint.checkers import BaseChecker


class DbfsSqlChecker(BaseChecker):
    """
    placeholder - this is a checker, not a plugin - will change to plugin
    """

    name = 'dbfs-sql'
    msgs = {
        'DB0001': (
            "Depecrecated use of mount points in SQL queries.",
            'deprecated-mount',
            """The use of mount points in SQL queries is deprecated and should be replaced with the appropriate
            blah balh
            """,
        ),
    }

    def visit_constant(self, node: nodes.Call) -> None:
        """
        placeholder
        """


def register(linter):
    linter.register_checker(DbfsSqlChecker(linter))
