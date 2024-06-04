from astroid import Attribute, Call, Name  # type: ignore


class AstHelper:
    @staticmethod
    def get_full_attribute_name(node: Attribute) -> str:
        return AstHelper._get_attribute_value(node)

    @staticmethod
    def get_full_function_name(node: Call) -> str | None:
        if not isinstance(node, Call):
            return None
        if isinstance(node.func, Attribute):
            return AstHelper._get_attribute_value(node.func)
        if isinstance(node.func, Name):
            return node.func.name
        return None

    @staticmethod
    def _get_attribute_value(node: Attribute):
        if isinstance(node.expr, Name):
            return node.expr.name + '.' + node.attrname
        if isinstance(node.expr, Attribute):
            return AstHelper._get_attribute_value(node.expr) + '.' + node.attrname
        if isinstance(node.expr, Call):
            name = AstHelper.get_full_function_name(node.expr)
            return node.attrname if name is None else name + '.' + node.attrname
        return None
