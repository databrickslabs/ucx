import logging

from astroid import Attribute, Call, Name  # type: ignore

logger = logging.getLogger(__file__)

missing_handlers: set[str] = set()


class AstHelper:
    @classmethod
    def get_full_attribute_name(cls, node: Attribute) -> str:
        return cls._get_attribute_value(node)

    @classmethod
    def get_full_function_name(cls, node: Call) -> str | None:
        if not isinstance(node, Call):
            return None
        if isinstance(node.func, Attribute):
            return cls._get_attribute_value(node.func)
        if isinstance(node.func, Name):
            return node.func.name
        return None

    @classmethod
    def _get_attribute_value(cls, node: Attribute):
        if isinstance(node.expr, Name):
            return node.expr.name + '.' + node.attrname
        if isinstance(node.expr, Attribute):
            parent = cls._get_attribute_value(node.expr)
            return node.attrname if parent is None else parent + '.' + node.attrname
        if isinstance(node.expr, Call):
            name = cls.get_full_function_name(node.expr)
            return node.attrname if name is None else name + '.' + node.attrname
        name = type(node.expr).__name__
        if name not in missing_handlers:
            missing_handlers.add(name)
            logger.debug(f"Missing handler for {name}")
        return None
