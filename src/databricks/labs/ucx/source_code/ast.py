import ast


class AstHelper:
    @staticmethod
    def get_full_attribute_name(node: ast.Attribute) -> str:
        return AstHelper._get_value(node)

    @staticmethod
    def get_full_function_name(node: ast.Call) -> str | None:
        if isinstance(node.func, ast.Attribute):
            return AstHelper._get_value(node.func)

        if isinstance(node.func, ast.Name):
            return node.func.id

        return None

    @staticmethod
    def _get_value(node: ast.Attribute):
        if isinstance(node.value, ast.Name):
            return node.value.id + '.' + node.attr

        if isinstance(node.value, ast.Attribute):
            return AstHelper._get_value(node.value) + '.' + node.attr

        return None
