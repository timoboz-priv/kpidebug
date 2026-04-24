from kpidebug.data.types import Row, TableFilter


def matches_filter(row: Row, f: TableFilter) -> bool:
    val = str(row.get(f.column, ""))
    target = f.value
    if f.operator == "eq":
        return val == target
    elif f.operator == "neq":
        return val != target
    elif f.operator == "contains":
        return target.lower() in val.lower()
    elif f.operator in ("gt", "gte", "lt", "lte"):
        try:
            fval, ftarget = float(val), float(target)
        except ValueError:
            fval, ftarget = None, None
        if fval is not None and ftarget is not None:
            if f.operator == "gt":
                return fval > ftarget
            elif f.operator == "gte":
                return fval >= ftarget
            elif f.operator == "lt":
                return fval < ftarget
            elif f.operator == "lte":
                return fval <= ftarget
        return val > target if f.operator in ("gt", "gte") else val < target
    return True


def apply_filters(rows: list[Row], filters: list[TableFilter]) -> list[Row]:
    for f in filters:
        rows = [r for r in rows if matches_filter(r, f)]
    return rows
