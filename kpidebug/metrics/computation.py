import ast
import operator

from kpidebug.data.types import Row


class ComputationError(Exception):
    pass


ALLOWED_FUNCTIONS = {"sum", "count", "avg", "min_val", "max_val", "ratio"}

BINARY_OPS: dict[type, operator] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
}

UNARY_OPS: dict[type, operator] = {
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}


def _values_for_field(rows: list[Row], field_name: str) -> list[float]:
    return [float(r.get(field_name, 0) or 0) for r in rows if field_name in r]


def _builtin_sum(rows: list[Row], field_name: str) -> float:
    return sum(_values_for_field(rows, field_name))


def _builtin_count(rows: list[Row], field_name: str) -> float:
    return float(len([r for r in rows if field_name in r]))


def _builtin_avg(rows: list[Row], field_name: str) -> float:
    values = _values_for_field(rows, field_name)
    if not values:
        return 0.0
    return sum(values) / len(values)


def _builtin_min_val(rows: list[Row], field_name: str) -> float:
    values = _values_for_field(rows, field_name)
    if not values:
        return 0.0
    return min(values)


def _builtin_max_val(rows: list[Row], field_name: str) -> float:
    values = _values_for_field(rows, field_name)
    if not values:
        return 0.0
    return max(values)


def _builtin_ratio(rows: list[Row], field_a: str, field_b: str) -> float:
    numerator = _builtin_sum(rows, field_a)
    denominator = _builtin_sum(rows, field_b)
    if denominator == 0.0:
        return 0.0
    return numerator / denominator


DSL_FUNCTIONS: dict[str, int] = {
    "sum": 1,
    "count": 1,
    "avg": 1,
    "min_val": 1,
    "max_val": 1,
    "ratio": 2,
}


def validate(expression: str) -> None:
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as e:
        raise ComputationError(f"Invalid expression syntax: {e}")

    _validate_node(tree.body)


def _validate_node(node: ast.AST) -> None:
    if isinstance(node, ast.Constant):
        if not isinstance(node.value, (int, float, str)):
            raise ComputationError(f"Unsupported constant type: {type(node.value).__name__}")

    elif isinstance(node, ast.BinOp):
        if type(node.op) not in BINARY_OPS:
            raise ComputationError(f"Unsupported operator: {type(node.op).__name__}")
        _validate_node(node.left)
        _validate_node(node.right)

    elif isinstance(node, ast.UnaryOp):
        if type(node.op) not in UNARY_OPS:
            raise ComputationError(f"Unsupported unary operator: {type(node.op).__name__}")
        _validate_node(node.operand)

    elif isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise ComputationError("Only direct function calls are allowed")
        if node.func.id not in DSL_FUNCTIONS:
            raise ComputationError(f"Unknown function: {node.func.id}")
        expected_args = DSL_FUNCTIONS[node.func.id]
        if len(node.args) != expected_args:
            raise ComputationError(
                f"{node.func.id}() expects {expected_args} argument(s), got {len(node.args)}"
            )
        for arg in node.args:
            if not isinstance(arg, ast.Constant) or not isinstance(arg.value, str):
                raise ComputationError(f"Arguments to {node.func.id}() must be string literals")
        if node.keywords:
            raise ComputationError("Keyword arguments are not supported")

    else:
        raise ComputationError(f"Unsupported expression element: {type(node).__name__}")


def evaluate(expression: str, rows: list[Row]) -> float:
    validate(expression)

    tree = ast.parse(expression, mode="eval")
    result = _eval_node(tree.body, rows)

    if not isinstance(result, (int, float)):
        raise ComputationError(f"Expression must evaluate to a number, got {type(result).__name__}")

    return float(result)


def _eval_node(node: ast.AST, rows: list[Row]) -> float:
    if isinstance(node, ast.Constant):
        return float(node.value)

    elif isinstance(node, ast.BinOp):
        left = _eval_node(node.left, rows)
        right = _eval_node(node.right, rows)
        op_fn = BINARY_OPS[type(node.op)]
        if isinstance(node.op, ast.Div) and right == 0.0:
            return 0.0
        return op_fn(left, right)

    elif isinstance(node, ast.UnaryOp):
        operand = _eval_node(node.operand, rows)
        op_fn = UNARY_OPS[type(node.op)]
        return op_fn(operand)

    elif isinstance(node, ast.Call):
        func_name = node.func.id
        args = [arg.value for arg in node.args]

        if func_name == "sum":
            return _builtin_sum(rows, args[0])
        elif func_name == "count":
            return _builtin_count(rows, args[0])
        elif func_name == "avg":
            return _builtin_avg(rows, args[0])
        elif func_name == "min_val":
            return _builtin_min_val(rows, args[0])
        elif func_name == "max_val":
            return _builtin_max_val(rows, args[0])
        elif func_name == "ratio":
            return _builtin_ratio(rows, args[0], args[1])

    raise ComputationError(f"Cannot evaluate node: {type(node).__name__}")
