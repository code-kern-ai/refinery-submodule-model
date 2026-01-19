from typing import Dict, Optional
from sqlglot import parse_one, tokenize
from sqlglot.errors import ParseError
from sqlglot import expressions as exp

from sqlglot import TokenType

# relative import so it works in console and in module
from .constants import (
    ALLOWED_NODES,
    ALLOWED_TOKEN_TYPES,
    ALLOWED_COLUMN_PREFIX,
    DISALLOWED_COLUMN_PREFIX,
)


def validate_sql_clause(
    select: Optional[str] = None,
    where: Optional[str] = None,
    group_by: Optional[str] = None,
    order_by: Optional[str] = None,
    include_db_check: bool = False,
    extend_allowed_nodes: Optional[set] = None,
) -> str | None | Dict[str, Optional[str]]:
    """
    Validate a user-provided clause.
    Returns None if safe, otherwise a string reason for rejection.
    """
    provided_clauses = list(filter(None, [select, where, order_by, group_by]))
    full_text_search_validation = (
        len(provided_clauses) == 1 and extend_allowed_nodes is None
    )
    if len(provided_clauses) == 0:
        return "No SELECT, WHERE or ORDER BY clause provided"
    elif len(provided_clauses) > 1 and extend_allowed_nodes is None:
        return "Only one of SELECT, WHERE, ORDER BY, or GROUP BY clauses can be provided at a time"
    elif len(provided_clauses) > 1 and extend_allowed_nodes is not None:
        all_results = {}
        params = {
            "select": select,
            "where": where,
            "group_by": group_by,
            "order_by": order_by,
        }
        for key in params:
            val = params.get(key)
            if val:
                deny_reason = validate_sql_clause(
                    **{key: val},
                    include_db_check=False,
                    extend_allowed_nodes=extend_allowed_nodes,
                )
            else:
                deny_reason = None
            all_results[key] = deny_reason
        if not any(all_results.values()) and include_db_check:
            # db check only if all clauses are valid
            full_sql = """
            SELECT {select} 
            FROM public.record r
            WHERE project_id = '00000000-0000-0000-0000-000000000000' {where}
            {group_by} 
            {order_by}
            LIMIT 0 """
            for key in params:
                if params[key] is None:
                    if key == "select":
                        params[key] = "1"
                    else:
                        params[key] = ""
                elif key == "group_by" and params[key]:
                    params[key] = "GROUP BY " + params[key]
                elif key == "order_by" and params[key]:
                    params[key] = "ORDER BY " + params[key]
            full_sql = full_sql.format(**params)
            try:
                from submodules.model.business_objects import general

                general.execute_all(full_sql)
                all_results["db_check"] = None
            except Exception as e:
                general.rollback()
                all_results["db_check"] = f"Database error when validating clauses: {e}"

        return all_results

    what = (
        "SELECT"
        if select
        else "WHERE"
        if where
        else "GROUP BY"
        if group_by
        else "ORDER BY"
    )
    # Step 1: reject unsafe tokens
    if reason := __contains_disallowed_tokens(select or where or group_by or order_by):
        return reason

    # Step 2: parse the clause in context
    try:
        if select:
            if full_text_search_validation:
                parsed = parse_one(select, read="postgres")
            else:
                parsed = parse_one(f"SELECT {select}", read="postgres")
        elif where:
            if full_text_search_validation:
                parsed = parse_one(where, read="postgres")
            else:
                # Wrap in SELECT WHERE to parse correctly as a single context
                parsed = parse_one(f"SELECT 1 WHERE {where}", read="postgres")
        elif group_by:
            if full_text_search_validation:
                parsed = parse_one(group_by, read="postgres")
            else:
                parsed = parse_one(f"SELECT 1 GROUP BY {group_by}", read="postgres")
        elif order_by:
            if full_text_search_validation:
                parsed = parse_one(order_by, read="postgres")
            else:
                # Wrap in SELECT ORDER BY to handle comma-separated lists correctly
                parsed = parse_one(f"SELECT 1 ORDER BY {order_by}", read="postgres")
    except ParseError:
        return f"Parse error => invalid {what} condition, check for correct syntax"

    if not parsed:
        return f"Invalid {what} clause"
    if full_text_search_validation:
        if reason := __contains_always_true(parsed):
            return reason
    else:
        for e in parsed.expressions:
            if reason := __contains_always_true(e):
                return reason

    # Step 3: walk AST nodes
    for node in parsed.walk():
        # Disallow sub-selects: only one Select node allowed, and it must be the root
        if isinstance(node, (exp.Select, exp.Subquery)) and node is not parsed:
            return "Sub-selects are not allowed"

        if node.key not in ALLOWED_NODES.union(extend_allowed_nodes or set()):
            return f"Disallowed node: {node.key}"

        if node.key == "column":
            if not str(node).startswith(ALLOWED_COLUMN_PREFIX):
                return f"Column does not start with allowed prefix: {str(node)}"

    if include_db_check:
        # import here to avoid test file dependency issues
        from submodules.model.business_objects import general

        try:
            if select:
                general.execute_all(
                    "SELECT " + select + " FROM public.record r LIMIT 0"
                )
            elif where:
                general.execute_all(
                    "SELECT 1 FROM public.record r WHERE " + where + " LIMIT 0"
                )
            elif order_by:
                general.execute_all(
                    "SELECT 1 FROM public.record r ORDER BY " + order_by + " LIMIT 0"
                )
            elif group_by:
                general.execute_all(
                    "SELECT 1 FROM public.record r GROUP BY " + group_by + " LIMIT 0"
                )
        except Exception as e:
            general.rollback()
            return f"Database error when validating {what} clause: {e}"
    return None


def __contains_disallowed_tokens(sql: str) -> str | None:
    """
    Check for comments or dangerous system functions.
    Returns a string reason if disallowed, None otherwise.
    """
    try:
        for t in tokenize(sql):
            if t.token_type not in ALLOWED_TOKEN_TYPES:
                return f"Disallowed token type: {t.text}"
            if (
                t.token_type == TokenType.IDENTIFIER or t.token_type == TokenType.VAR
            ) and t.text.lower().startswith(DISALLOWED_COLUMN_PREFIX):
                return f"Disallowed system function: {t.text}"
            if t.comments:
                return "Comments are not allowed"
    except Exception as e:
        return f"Tokenization error - invalid WHERE condition: {str(e)}"
    return None


def __contains_always_true(expr):
    """
    Return a string reason if expr is provably always-true, otherwise None.

    Rules covered:
     - A top-level Boolean literal TRUE (exp.Boolean) -> always-true
     - A CAST to BOOLEAN whose inner is a literal 1/TRUE -> always-true
       (but NOT a cast of a column/expression)
     - EQ of two identical literals (1 = 1, 'a' = 'a') -> always-true
     - OR: if any branch is always-true -> always-true
     - AND: if both branches are always-true -> always-true
     - Parentheses are unwrapped
    """
    # Unwrap parentheses
    if isinstance(expr, exp.Paren):
        inner = expr.args.get("this")
        if isinstance(inner, exp.Expression):
            return __contains_always_true(inner)

    # EQ: literal = literal (both sides must be literals and equal)
    if isinstance(expr, exp.EQ):
        left = expr.args.get("this")
        right = expr.args.get("expression")
        if isinstance(left, exp.Literal) and isinstance(right, exp.Literal):
            # For safety compare their `.this` representation; adapt if you need type-aware compare
            if left.this == right.this:
                return f"Always-true expression: {left.this} = {right.this}"
        # Do NOT recurse into left/right here — a literal RHS TRUE does not make the EQ always true.

    # Cast to BOOLEAN of a literal (like 1::BOOLEAN)
    if isinstance(expr, exp.Cast):
        to_type = expr.args.get("to")
        inner = expr.args.get("this")
        # to_type could be an Identifier, DataType, or other node; string compare is pragmatic
        if to_type and isinstance(inner, exp.Literal):
            try:
                typ = str(to_type).upper()
            except Exception:
                typ = ""
            if "BOOLEAN" in typ:
                # Treat literal 1 / '1' / True / 'TRUE' as always-true (adapt to your dialect needs)
                if inner.this in (1, "1", True, "TRUE"):
                    return f"Always-true cast: {inner.this}::BOOLEAN"
        # Do NOT treat casts of non-literals as always-true.

    # Top-level Boolean literal: only flag if expr itself is the Boolean node
    if isinstance(expr, exp.Boolean):
        if expr.this:  # True value
            return f"Always-true boolean literal: {expr.this}"
        # If it's FALSE, it's not always-true; ignore.

    # OR: any branch always-true => whole expression always-true
    if isinstance(expr, exp.Or):
        left = expr.args.get("this")
        right = expr.args.get("expression")
        if left and isinstance(left, exp.Expression):
            if reason := __contains_always_true(left):
                return reason
        if right and isinstance(right, exp.Expression):
            if reason := __contains_always_true(right):
                return reason

    # AND: both branches must be always-true
    if isinstance(expr, exp.And):
        left = expr.args.get("this")
        right = expr.args.get("expression")
        left_reason = (
            __contains_always_true(left) if isinstance(left, exp.Expression) else None
        )
        right_reason = (
            __contains_always_true(right) if isinstance(right, exp.Expression) else None
        )
        if left_reason and right_reason:
            return f"Always-true AND expression: {left_reason} and {right_reason}"

    # For all other node types, do NOT descend into arbitrary children:
    # only recurse into child expressions that are meaningful for short-circuit logic.
    # This prevents a Boolean literal nested as part of a larger expression from being treated as the whole expression.
    return None
