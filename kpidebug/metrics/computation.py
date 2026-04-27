"""SQL-like DSL for metric computation.

Syntax:
    sum(amount) from charges
    count() from customers
    avg(amount) from charges where status = "paid"
    ratio(count() from charges where status = "succeeded", count() from charges)
    sum(amount) from charges where status = "paid" and amount > "100"

Without `from`: operates on a default table (for backward compat).

Grammar:
    expression := ratio_expr | arithmetic_expr
    arithmetic_expr := aggregate_expr (('+' | '-' | '*' | '/') aggregate_expr)*
    aggregate_expr := AGG_FN '(' field? ')' ['from' table_name] [where_clause]
                    | '(' expression ')'
                    | NUMBER
    ratio_expr := 'ratio' '(' expression ',' expression ')'
    where_clause := 'where' condition ('and' condition)*
    condition := field COMP_OP value
    AGG_FN := 'sum' | 'count' | 'avg' | 'min' | 'max'
    COMP_OP := '=' | '!=' | '>' | '>=' | '<' | '<=' | 'contains'
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field as dataclass_field
from typing import TYPE_CHECKING

from kpidebug.data.table import DataTable
from kpidebug.data.table_memory import InMemoryDataTable
from kpidebug.data.types import Aggregation, Row, TableDescriptor

if TYPE_CHECKING:
    from kpidebug.metrics.context import MetricContext


class ComputationError(Exception):
    pass


AGG_FUNCTIONS = {"sum", "count", "avg", "min", "max"}
COMP_OPS = {"=": "eq", "!=": "neq", ">": "gt", ">=": "gte", "<": "lt", "<=": "lte", "contains": "contains"}


@dataclass
class _Token:
    type: str
    value: str


def _tokenize(expr: str) -> list[_Token]:
    tokens: list[_Token] = []
    i = 0
    while i < len(expr):
        if expr[i].isspace():
            i += 1
            continue

        if expr[i] == '"':
            j = expr.index('"', i + 1)
            tokens.append(_Token("STRING", expr[i + 1:j]))
            i = j + 1
            continue

        if expr[i] in "(),+-*/":
            tokens.append(_Token(expr[i], expr[i]))
            i += 1
            continue

        if expr[i:i + 2] in (">=", "<=", "!="):
            tokens.append(_Token("COMP_OP", expr[i:i + 2]))
            i += 2
            continue
        if expr[i] in "=><":
            tokens.append(_Token("COMP_OP", expr[i]))
            i += 1
            continue

        m = re.match(r"[0-9]+(\.[0-9]+)?", expr[i:])
        if m:
            tokens.append(_Token("NUMBER", m.group()))
            i += m.end()
            continue

        m = re.match(r"[a-zA-Z_][a-zA-Z0-9_.]*", expr[i:])
        if m:
            word = m.group()
            if word in AGG_FUNCTIONS or word == "ratio":
                tokens.append(_Token("FUNC", word))
            elif word in ("where", "and", "contains", "from"):
                tokens.append(_Token(word.upper(), word))
            else:
                tokens.append(_Token("IDENT", word))
            i += m.end()
            continue

        raise ComputationError(f"Unexpected character: {expr[i]}")

    return tokens


class _Parser:
    _tokens: list[_Token]
    _pos: int

    def __init__(self, tokens: list[_Token]):
        self._tokens = tokens
        self._pos = 0

    def _peek(self) -> _Token | None:
        if self._pos < len(self._tokens):
            return self._tokens[self._pos]
        return None

    def _advance(self) -> _Token:
        t = self._tokens[self._pos]
        self._pos += 1
        return t

    def _expect(self, token_type: str) -> _Token:
        t = self._peek()
        if t is None or t.type != token_type:
            expected = token_type
            got = t.value if t else "end of expression"
            raise ComputationError(f"Expected {expected}, got {got}")
        return self._advance()

    def parse(self) -> _Node:
        node = self._parse_expression()
        if self._pos < len(self._tokens):
            raise ComputationError(f"Unexpected token: {self._tokens[self._pos].value}")
        return node

    def _parse_expression(self) -> _Node:
        t = self._peek()
        if t and t.type == "FUNC" and t.value == "ratio":
            return self._parse_ratio()
        return self._parse_arithmetic()

    def _parse_arithmetic(self) -> _Node:
        left = self._parse_primary()
        while True:
            t = self._peek()
            if t and t.type in ("+", "-", "*", "/"):
                op = self._advance().value
                right = self._parse_primary()
                left = _ArithNode(op=op, left=left, right=right)
            else:
                break
        return left

    def _parse_primary(self) -> _Node:
        t = self._peek()
        if t is None:
            raise ComputationError("Unexpected end of expression")

        if t.type == "(":
            self._advance()
            node = self._parse_expression()
            self._expect(")")
            return node

        if t.type == "NUMBER":
            self._advance()
            return _LiteralNode(value=float(t.value))

        if t.type == "FUNC":
            return self._parse_aggregate()

        raise ComputationError(f"Unexpected token: {t.value}")

    def _parse_aggregate(self) -> _AggNode:
        func = self._advance()
        self._expect("(")
        field = ""
        t = self._peek()
        if t and t.type == "IDENT":
            field = self._advance().value
        self._expect(")")

        table_name = ""
        if self._peek() and self._peek().type == "FROM":
            self._advance()
            table_name = self._expect("IDENT").value

        filters: list[tuple[str, str, str]] = []
        if self._peek() and self._peek().type == "WHERE":
            self._advance()
            filters = self._parse_conditions()

        method = {
            "sum": Aggregation.SUM,
            "count": Aggregation.COUNT,
            "avg": Aggregation.AVG,
            "min": Aggregation.MIN,
            "max": Aggregation.MAX,
        }[func.value]

        return _AggNode(method=method, field=field, table_name=table_name, filters=filters)

    def _parse_conditions(self) -> list[tuple[str, str, str]]:
        conditions = [self._parse_condition()]
        while self._peek() and self._peek().type == "AND":
            self._advance()
            conditions.append(self._parse_condition())
        return conditions

    def _parse_condition(self) -> tuple[str, str, str]:
        field = self._expect("IDENT").value

        t = self._peek()
        if t and t.type == "CONTAINS":
            self._advance()
            op = "contains"
        elif t and t.type == "COMP_OP":
            op = COMP_OPS[self._advance().value]
        else:
            raise ComputationError(f"Expected comparison operator after {field}")

        val_token = self._peek()
        if val_token and val_token.type == "STRING":
            val = self._advance().value
        elif val_token and val_token.type == "NUMBER":
            val = self._advance().value
        elif val_token and val_token.type == "IDENT":
            val = self._advance().value
        else:
            raise ComputationError(f"Expected value after {field} {op}")

        return (field, op, val)

    def _parse_ratio(self) -> _RatioNode:
        self._advance()
        self._expect("(")
        numerator = self._parse_expression()
        self._expect(",")
        denominator = self._parse_expression()
        self._expect(")")
        return _RatioNode(numerator=numerator, denominator=denominator)


class _Node:
    pass


@dataclass
class _AggNode(_Node):
    method: Aggregation = Aggregation.SUM
    field: str = ""
    table_name: str = ""
    filters: list[tuple[str, str, str]] = dataclass_field(default_factory=list)


@dataclass
class _LiteralNode(_Node):
    value: float = 0.0


@dataclass
class _ArithNode(_Node):
    op: str = "+"
    left: _Node = dataclass_field(default_factory=_Node)
    right: _Node = dataclass_field(default_factory=_Node)


@dataclass
class _RatioNode(_Node):
    numerator: _Node = dataclass_field(default_factory=_Node)
    denominator: _Node = dataclass_field(default_factory=_Node)


def _get_table(node: _AggNode, ctx: MetricContext | None, default_table: DataTable | None) -> DataTable:
    if node.table_name and ctx:
        return ctx.table(node.table_name)
    if default_table is not None:
        return default_table
    raise ComputationError("No table specified and no default table available")


def _eval_node(node: _Node, ctx: MetricContext | None, default_table: DataTable | None) -> float:
    if isinstance(node, _LiteralNode):
        return node.value

    if isinstance(node, _AggNode):
        t = _get_table(node, ctx, default_table)
        for field, op, val in node.filters:
            t = t.filter(field, op, val)
        return t.aggregate(node.field, node.method)

    if isinstance(node, _ArithNode):
        left = _eval_node(node.left, ctx, default_table)
        right = _eval_node(node.right, ctx, default_table)
        if node.op == "+":
            return left + right
        elif node.op == "-":
            return left - right
        elif node.op == "*":
            return left * right
        elif node.op == "/":
            return left / right if right != 0 else 0.0

    if isinstance(node, _RatioNode):
        num = _eval_node(node.numerator, ctx, default_table)
        den = _eval_node(node.denominator, ctx, default_table)
        return num / den if den != 0 else 0.0

    raise ComputationError(f"Unknown node type: {type(node).__name__}")


def validate(expression: str) -> None:
    tokens = _tokenize(expression)
    parser = _Parser(tokens)
    parser.parse()


def evaluate(expression: str, rows: list[Row]) -> float:
    tokens = _tokenize(expression)
    parser = _Parser(tokens)
    ast = parser.parse()
    table = InMemoryDataTable(TableDescriptor(), rows)
    return _eval_node(ast, None, table)


def evaluate_with_context(expression: str, ctx: MetricContext) -> float:
    tokens = _tokenize(expression)
    parser = _Parser(tokens)
    ast = parser.parse()
    return _eval_node(ast, ctx, None)
