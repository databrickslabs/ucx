from astroid import nodes  # type: ignore
from pylint.checkers import BaseChecker


class TestingChecker(BaseChecker):
    name = 'testing'
    msgs = {
        'R9001': (
            "Obscure implicit test dependency with mock.patch(%s). Rewrite to inject dependencies through constructor.",
            'prohibited-patch',
            """Using `patch` to mock dependencies in unit tests can introduce implicit dependencies within a class, 
            making it unclear to other developers. Constructor arguments, on the other hand, explicitly declare 
            dependencies, enhancing code readability and maintainability. However, reliance on `patch` for testing may 
            lead to issues during refactoring, as updates to underlying implementations would necessitate changes across
            multiple unrelated unit tests. Moreover, the use of hard-coded strings in `patch` can obscure which unit 
            tests require modification, as they lack strongly typed references. This coupling of the class under test 
            to concrete classes signifies a code smell, and such code is not easily portable to statically typed 
            languages where monkey patching isn't feasible without significant effort. In essence, extensive patching of 
            external clients suggests a need for refactoring, with experienced engineers recognizing the potential for 
            dependency inversion in such scenarios.""",
        ),
        'R9002': (
            "Obscure implicit test dependency with MagicMock(). Rewrite with create_autospec(ConcreteType).",
            'obscure-mock',
            """Using `MagicMock` to mock dependencies in unit tests can introduce implicit dependencies within a class,
            making it unclear to other developers. create_autospec(ConcreteType) is a better alternative, as it
            automatically creates a mock object with the same attributes and methods as the concrete class. This
            approach ensures that the mock object behaves like the concrete class, allowing for more robust and
            maintainable unit tests. Moreover, reliance on `MagicMock` for testing leads to issues during refactoring,
            as updates to underlying implementations would necessitate changes across multiple unrelated unit tests.""",
        ),
    }

    def visit_call(self, node: nodes.Call) -> None:
        # this also means that rare cases, like MagicMock(side_effect=...) are fine
        if node.as_string() == 'MagicMock()':
            # here we can go and figure out the expected type of the object being mocked based on the arguments
            # where it is being assigned to, but that is a bit too much for this check. Other people can add this later.
            self.add_message('obscure-mock', node=node)
        if not node.args:
            return
        if node.func.as_string() in ('mocker.patch', 'patch'):
            argument_value = node.args[0].as_string()
            no_quotes = argument_value.strip("'\"")
            if no_quotes.startswith("databricks."):
                self.add_message('prohibited-patch', node=node, args=argument_value)


def register(linter):
    linter.register_checker(TestingChecker(linter))
