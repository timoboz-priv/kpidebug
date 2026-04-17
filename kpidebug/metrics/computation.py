import ast
import operator

from kpidebug.metrics.types import DataRecord


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


def _records_for_field(records: list[DataRecord], field_name: str) -> list[DataRecord]:
    return [r for r in records if r.field == field_name]


def _builtin_sum(records: list[DataRecord], field_name: str) -> float:
    matching = _records_for_field(records, field_name)
    return sum(r.value for r in matching)


def _builtin_count(records: list[DataRecord], field_name: str) -> float:
    matching = _records_for_field(records, field_name)
    return float(len(matching))


def _builtin_avg(records: list[DataRecord], field_name: str) -> float:
    matching = _records_for_field(records, field_name)
    if not matching:
        return 0.0
    return sum(r.value for r in matching) / len(matching)


def _builtin_min_val(records: list[DataRecord], field_name: str) -> float:
    matching = _records_for_field(records, field_name)
    if not matching:
        return 0.0
    return min(r.value for r in matching)


def _builtin_max_val(records: list[DataRecord], field_name: str) -> float:
    matching = _records_for_field(records, field_name)
    if not matching:
        return 0.0
    return max(r.value for r in matching)


def _builtin_ratio(records: list[DataRecord], field_a: str, field_b: str) -> float:
    numerator = _builtin_sum(records, field_a)
    denominator = _builtin_sum(records, field_b)
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
    """Validate that an expression is safe to evaluate. Raises ComputationError if not."""
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


def evaluate(expression: str, records: list[DataRecord]) -> float:
    """Evaluate a DSL expression against a list of data records.

    The expression can use:
    - sum('field'), count('field'), avg('field'), min_val('field'), max_val('field')
    - ratio('field_a', 'field_b')
    - Arithmetic operators: +, -, *, /
    - Numeric literals
    """
    validate(expression)

    tree = ast.parse(expression, mode="eval")
    result = _eval_node(tree.body, records)

    if not isinstance(result, (int, float)):
        raise ComputationError(f"Expression must evaluate to a number, got {type(result).__name__}")

    return float(result)


def _eval_node(node: ast.AST, records: list[DataRecord]) -> float:
    if isinstance(node, ast.Constant):
        return float(node.value)

    elif isinstance(node, ast.BinOp):
        left = _eval_node(node.left, records)
        right = _eval_node(node.right, records)
        op_fn = BINARY_OPS[type(node.op)]
        if isinstance(node.op, ast.Div) and right == 0.0:
            return 0.0
        return op_fn(left, right)

    elif isinstance(node, ast.UnaryOp):
        operand = _eval_node(node.operand, records)
        op_fn = UNARY_OPS[type(node.op)]
        return op_fn(operand)

    elif isinstance(node, ast.Call):
        func_name = node.func.id
        args = [arg.value for arg in node.args]

        if func_name == "sum":
            return _builtin_sum(records, args[0])
        elif func_name == "count":
            return _builtin_count(records, args[0])
        elif func_name == "avg":
            return _builtin_avg(records, args[0])
        elif func_name == "min_val":
            return _builtin_min_val(records, args[0])
        elif func_name == "max_val":
            return _builtin_max_val(records, args[0])
        elif func_name == "ratio":
            return _builtin_ratio(records, args[0], args[1])

    raise ComputationError(f"Cannot evaluate node: {type(node).__name__}")
