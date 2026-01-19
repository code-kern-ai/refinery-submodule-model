from typing import Dict, Optional
from sqlglot import parse_one, tokenize
from sqlglot.errors import ParseError
from sqlglot import expressions as exp
from sqlglot.optimizer.simplify import simplify

from sqlglot import TokenType

# relative import so it works in console and in module
from .constants import (
    ALLOWED_NODES,
    ALLOWED_TOKEN_TYPES,
    ALLOWED_COLUMN_PREFIX,
    DISALLOWED_COLUMN_PREFIX,
    ALLOWED_FUNCS,
)


def validate_sql_clause(
    select: Optional[str] = None,
    where: Optional[str] = None,
    group_by: Optional[str] = None,
    order_by: Optional[str] = None,
    include_db_check: bool = False,
    extend_allowed_nodes: Optional[set] = None,
    disallowed_identifiers: Optional[set] = None,
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
                    disallowed_identifiers=disallowed_identifiers,
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
        else "WHERE" if where else "GROUP BY" if group_by else "ORDER BY"
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

    # Step 3: walk AST nodes
    for node in parsed.walk():
        # Disallow sub-selects: only one Select node allowed, and it must be the root
        if isinstance(node, (exp.Select, exp.Subquery)) and node is not parsed:
            return "Sub-selects are not allowed"

        if node.key not in ALLOWED_NODES.union(extend_allowed_nodes or set()):
            return f"Disallowed node: {node.key}"

        if node.key == "column":
            # Strict column check: no quotes, no extra spaces, exact case
            full_col = str(node)
            if not full_col.startswith(ALLOWED_COLUMN_PREFIX):
                return f"Column does not start with allowed prefix: {full_col}"

        if node.key == "identifier":
            full_col = str(node)
            if full_col.endswith(tuple(disallowed_identifiers)):
                return f"Disallowed identifier: {full_col}"

        # Reject comparisons where left side is identical to right side (e.g. 1=1, data=data)
        if isinstance(node, exp.Binary) and not isinstance(
            node, (exp.JSONExtract, exp.JSONExtractScalar)
        ):
            # Normalize sides for comparison to catch "data" = data
            left_norm = str(node.left).replace('"', "").replace(" ", "").lower()
            right_norm = str(node.right).replace('"', "").replace(" ", "").lower()
            if left_norm == right_norm:
                return f"Tautology detected: identical sides in {node.key}"

        # Strict function whitelisting for anonymous functions
        if isinstance(node, exp.Anonymous):
            func_name = node.this.lower()
            if func_name not in ALLOWED_FUNCS:
                return f"Disallowed function: {func_name}"

    # Robustness: Any WHERE clause must actually refer to a column
    where_node = None
    if where:
        if full_text_search_validation:
            where_node = parsed
        elif parsed.args.get("where"):
            where_node = parsed.args["where"].this

    if where_node:
        has_column = False
        for n in where_node.walk():
            # In some dialects/versions, sqlglot might parse JSON access as Lambda or other nodes
            # so we check for Column or Identifier nodes that look like our allowed columns
            if isinstance(n, (exp.Column, exp.Identifier)):
                name = n.name.lower() if hasattr(n, "name") else str(n).lower()
                if name in ["data", "r.data", "r"]:
                    has_column = True
                    break
        if not has_column:
            return "WHERE clause must contain at least one column reference"

    # Final check for always-true/constant expressions
    # Always check the original parsed tree for tautologies first
    if full_text_search_validation:
        if reason := __contains_always_true(parsed):
            return reason
    else:
        # Check WHERE clause and SELECT expressions separately
        if parsed.args.get("where"):
            if reason := __contains_always_true(parsed.args["where"].this):
                return reason
        for e in parsed.expressions:
            if reason := __contains_always_true(e):
                return reason

    # Now simplify and check again to catch things like NOT FALSE
    try:
        simplified = simplify(parsed)
        # If the WHERE clause disappeared during simplification, it was likely a tautology (e.g. 1=1)
        if parsed.args.get("where") and not simplified.args.get("where"):
            return "Tautology detected: WHERE clause simplified away"

        if full_text_search_validation:
            if reason := __contains_always_true(simplified):
                return reason
        else:
            if simplified.args.get("where"):
                if reason := __contains_always_true(simplified.args["where"].this):
                    return reason
            for e in simplified.expressions:
                if reason := __contains_always_true(e):
                    return reason
    except Exception:
        pass

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
    Return a string reason if expr is a constant (always-true or always-false),
    as we want to avoid constant conditions in WHERE/SELECT.
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
            if left.this == right.this:
                return f"Always-true expression: {left.this} = {right.this}"

    # Cast to BOOLEAN of a literal (like 1::BOOLEAN)
    if isinstance(expr, exp.Cast):
        to_type = expr.args.get("to")
        inner = expr.args.get("this")
        if to_type and isinstance(inner, exp.Literal):
            try:
                typ = str(to_type).upper()
            except Exception:
                typ = ""
            if "BOOLEAN" in typ:
                return f"Constant cast to BOOLEAN: {inner.this}::BOOLEAN"

    # Any Boolean literal (True or False)
    if isinstance(expr, exp.Boolean):
        return f"Constant boolean literal: {expr.this}"

    # OR: recurse to catch (1=1 OR ...)
    if isinstance(expr, exp.Or):
        left = expr.args.get("this")
        right = expr.args.get("expression")
        if left and isinstance(left, exp.Expression):
            if reason := __contains_always_true(left):
                return reason
        if right and isinstance(right, exp.Expression):
            if reason := __contains_always_true(right):
                return reason

    # COALESCE: if the first non-null is always true
    if isinstance(expr, exp.Coalesce):
        for arg in expr.expressions:
            if (
                isinstance(arg, exp.Literal)
                and arg.is_string
                and arg.this.lower() == "null"
            ):
                continue
            if isinstance(arg, exp.Null):
                continue
            # first non-null
            return __contains_always_true(arg)

    return None
