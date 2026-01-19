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
    MAX_WINDOW_FUNCTIONS,
    MAX_QUERY_LENGTH,
    MAX_EXPRESSION_DEPTH,
    MAX_REGEX_LENGTH,
    DANGEROUS_REGEX_PATTERNS,
)
import re


def validate_sql_clause(
    select: Optional[str] = None,
    where: Optional[str] = None,
    group_by: Optional[str] = None,
    order_by: Optional[str] = None,
    include_db_check: bool = False,
    extend_allowed_nodes: Optional[set] = None,
    db_check_tautology: bool = False,
    tautology_check_project_id: Optional[str] = None,
) -> str | None | Dict[str, Optional[str]]:
    """
    Validate a user-provided SQL clause for security.
    
    Returns None if safe, otherwise a string reason for rejection.
    
    Parameters:
        select: SELECT clause content (without SELECT keyword)
        where: WHERE clause content (without WHERE keyword)
        group_by: GROUP BY clause content (without GROUP BY keyword)
        order_by: ORDER BY clause content (without ORDER BY keyword)
        include_db_check: If True, validates syntax against the database
        extend_allowed_nodes: Additional AST nodes to allow
        db_check_tautology: If True, performs database-based tautology detection.
            This executes the WHERE condition against actual project data to detect
            conditions that always evaluate to TRUE. Requires tautology_check_project_id.
            Note: This adds query overhead proportional to project size.
        tautology_check_project_id: Project UUID required for db_check_tautology.
            The tautology check evaluates: SELECT DISTINCT ({where}) FROM record WHERE project_id = '{id}'
            If result is a single TRUE value, the condition is flagged as always-true.
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

        # Database-based tautology check for multi-clause validation
        if not any(all_results.values()) and db_check_tautology and where and tautology_check_project_id:
            tautology_result = validate_sql_clause(
                where=where,
                extend_allowed_nodes=extend_allowed_nodes,
                db_check_tautology=True,
                tautology_check_project_id=tautology_check_project_id,
            )
            if tautology_result:
                all_results["db_tautology_check"] = tautology_result

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

    # Step 0: Check query length and empty/whitespace content
    clause_text = select or where or group_by or order_by
    if not clause_text or not clause_text.strip():
        return f"Empty or whitespace-only {what} clause is not allowed"
    if len(clause_text) > MAX_QUERY_LENGTH:
        return f"Query too long: {len(clause_text)} characters (maximum allowed: {MAX_QUERY_LENGTH})"

    # Step 1: reject unsafe tokens
    if reason := __contains_disallowed_tokens(clause_text):
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
            # We always wrap GROUP BY and ORDER BY to handle comma-separated lists
            parsed = parse_one(f"SELECT 1 GROUP BY {group_by}", read="postgres")
        elif order_by:
            parsed = parse_one(f"SELECT 1 ORDER BY {order_by}", read="postgres")
    except ParseError:
        return f"Parse error => invalid {what} condition, check for correct syntax"

    if not parsed:
        return f"Invalid {what} clause"

    # CRITICAL SECURITY: Reject SELECT * unconditionally - we only allow access to the data JSON field
    # This prevents exposure of any columns other than the data JSONB field in public.record
    # IMPORTANT: * is NEVER allowed, even in count(*) - users must use count(1) instead
    # This is a security requirement to prevent accidental or intentional column exposure
    if select and isinstance(parsed, exp.Select):
        for expr in parsed.expressions:
            if isinstance(expr, exp.Star):
                return "SELECT * is not allowed - only the data JSON field can be accessed. Use count(1) instead of count(*)"

    # Complexity limit: count window functions to prevent DoS
    # Window functions are powerful but can be expensive; limit their count
    window_count = sum(1 for node in parsed.walk() if isinstance(node, exp.Window))
    if window_count > MAX_WINDOW_FUNCTIONS:
        return f"Too many window functions: {window_count} (maximum allowed: {MAX_WINDOW_FUNCTIONS})"

    # Complexity limit: check expression depth to prevent DoS via deeply nested expressions
    max_depth = __get_max_expression_depth(parsed)
    if max_depth > MAX_EXPRESSION_DEPTH:
        return f"Expression too deeply nested: depth {max_depth} (maximum allowed: {MAX_EXPRESSION_DEPTH})"

    # Check for dangerous regex patterns that could cause ReDoS
    if reason := __check_regex_complexity(parsed):
        return reason

    # Step 3: walk AST nodes
    for node in parsed.walk():
        # Disallow sub-selects: only one Select node allowed, and it must be the root
        if isinstance(node, (exp.Select, exp.Subquery)) and node is not parsed:
            return "Sub-selects are not allowed"

        # CRITICAL SECURITY: Reject ALL * (star) nodes unconditionally
        # * is NEVER allowed anywhere in the query - users must use count(1) instead of count(*)
        # This prevents any possibility of accessing columns other than the data JSON field
        # Even in aggregate functions, * could potentially expose column information
        if isinstance(node, exp.Star):
            return "* is not allowed - only the data JSON field can be accessed. Use count(1) instead of count(*)"

        if node.key not in ALLOWED_NODES.union(extend_allowed_nodes or set()):
            return f"Disallowed node: {node.key}"

        if node.key == "column":
            # Strict column check: no quotes, no extra spaces, exact case
            full_col = str(node)
            if not full_col.startswith(ALLOWED_COLUMN_PREFIX):
                return f"Column does not start with allowed prefix: {full_col}"

        # SECURITY: Reject dangerous PostgreSQL system type casts that could access system catalogs
        # Types like regclass, oid, etc. can be used to reference system catalog objects
        # This prevents potential information disclosure or system catalog access
        if isinstance(node, exp.Cast):
            to_type = node.args.get("to")
            if to_type:
                type_str = str(to_type).lower()
                # All PostgreSQL pseudo-types that reference system catalogs
                dangerous_types = [
                    "regclass",       # OID of relation
                    "regcollation",   # OID of collation
                    "regconfig",      # OID of text search config
                    "regdictionary",  # OID of text search dictionary
                    "regnamespace",   # OID of namespace
                    "regoper",        # OID of operator
                    "regoperator",    # OID of operator with types
                    "regproc",        # OID of function
                    "regprocedure",   # OID of function with types
                    "regrole",        # OID of role
                    "regtype",        # OID of type
                    "oid",            # Object identifier
                    "xid",            # Transaction ID
                    "cid",            # Command ID
                    "tid",            # Tuple ID (physical location)
                    "name",           # Internal name type
                    "refcursor",      # Cursor reference
                ]
                if any(dt in type_str for dt in dangerous_types):
                    return f"Dangerous system type cast not allowed: {type_str}"

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

            # SECURITY: Check JSON path functions for SQL keywords in path arguments
            # Even though these are string literals (not executed as SQL), we block them
            # as a defense-in-depth measure to prevent any potential confusion or edge cases
            if func_name.startswith("jsonb_path_"):
                for arg in node.expressions:
                    if isinstance(arg, exp.Literal) and arg.is_string:
                        path_str = arg.this.upper()
                        # Block any SQL keywords that could indicate injection attempts
                        sql_keywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "DROP", 
                                       "TRUNCATE", "ALTER", "CREATE", "EXECUTE", "UNION",
                                       "FROM", "WHERE", "JOIN", "INTO"]
                        for keyword in sql_keywords:
                            if keyword in path_str:
                                return f"SQL keyword '{keyword}' not allowed in JSON path expression"

    # Robustness: WHERE and GROUP BY clauses must actually refer to a column
    # ORDER BY can contain aggregates (like count(1)) which don't expose columns, so we're more lenient
    target_node = None
    if where:
        if full_text_search_validation:
            target_node = parsed
        elif parsed.args.get("where"):
            target_node = parsed.args["where"].this
    elif group_by:
        if parsed.args.get("group"):
            target_node = parsed.args["group"]

    if target_node:
        has_column = False
        for n in target_node.walk():
            # In some dialects/versions, sqlglot might parse JSON access as Lambda or other nodes
            # so we check for Column or Identifier nodes that look like our allowed columns
            # We also allow Literal (integers) as they can represent column indices (e.g. GROUP BY 1)
            # but only if they are the direct child of the GROUP BY (not part of an expression like 1/0)
            if isinstance(n, (exp.Column, exp.Identifier)):
                name = n.name.lower() if hasattr(n, "name") else str(n).lower()
                if name in ["data", "r.data", "r"]:
                    has_column = True
                    break
            if n is target_node or n.parent is target_node:
                if isinstance(n, exp.Literal) and n.is_number:
                    try:
                        val = int(n.this)
                        if val > 0:
                            has_column = True
                            break
                    except ValueError:
                        pass
        if not has_column:
            return f"{what} clause must contain at least one column reference"

    # ORDER BY: Must reference a column OR be an aggregate function (aggregates don't expose columns)
    if order_by and parsed.args.get("order"):
        order_node = parsed.args["order"]
        has_column_or_aggregate = False
        for expr in order_node.expressions:
            ordered_expr = expr.this if isinstance(expr, exp.Ordered) else expr
            # Check if it's an aggregate function (safe - doesn't expose columns)
            if isinstance(
                ordered_expr,
                (
                    exp.Count,
                    exp.Sum,
                    exp.Avg,
                    exp.Min,
                    exp.Max,
                    exp.Stddev,
                    exp.Variance,
                    exp.Corr,
                    exp.CovarPop,
                    exp.LogicalOr,
                    exp.LogicalAnd,
                    exp.BitwiseAndAgg,
                    exp.BitwiseOrAgg,
                    exp.ArrayAgg,
                    exp.JSONArrayAgg,
                    exp.AnyValue,
                ),
            ):
                has_column_or_aggregate = True
                break
            # Check if it references our allowed columns
            # We also allow standalone integer literals as column indices (e.g. ORDER BY 1)
            if isinstance(ordered_expr, exp.Literal) and ordered_expr.is_number:
                try:
                    val = int(ordered_expr.this)
                    if val > 0:
                        has_column_or_aggregate = True
                except ValueError:
                    pass
            
            if not has_column_or_aggregate:
                for n in ordered_expr.walk():
                    if isinstance(n, (exp.Column, exp.Identifier)):
                        name = n.name.lower() if hasattr(n, "name") else str(n).lower()
                        if name in ["data", "r.data", "r"]:
                            has_column_or_aggregate = True
                            break
                    if has_column_or_aggregate:
                        break
        if not has_column_or_aggregate:
            return "ORDER BY clause must contain at least one column reference or aggregate function"

    # Final check for always-true/constant expressions
    # Always check the original parsed tree for tautologies first
    if full_text_search_validation:
        if reason := __contains_always_true(parsed):
            return reason
    else:
        # Check WHERE clause, SELECT expressions, ORDER BY and GROUP BY separately
        if parsed.args.get("where"):
            if reason := __contains_always_true(parsed.args["where"].this):
                return reason
        if parsed.args.get("order"):
            for e in parsed.args["order"].expressions:
                # e is an Ordered expression, check its inner content
                inner = e.this if isinstance(e, exp.Ordered) else e
                if reason := __contains_always_true(inner):
                    return reason
        if parsed.args.get("group"):
            for e in parsed.args["group"].expressions:
                if reason := __contains_always_true(e):
                    return reason
        for e in parsed.expressions:
            if reason := __contains_always_true(e):
                return reason

        # Check for tautologies inside window function ORDER BY clauses
        for node in parsed.walk():
            if isinstance(node, exp.Window):
                window_order = node.args.get("order")
                if window_order:
                    for e in window_order.expressions:
                        inner = e.this if isinstance(e, exp.Ordered) else e
                        if reason := __contains_always_true(inner):
                            return f"Tautology in window ORDER BY: {reason}"

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

    # Database-based tautology detection (optional)
    # This evaluates the WHERE condition against actual project data to catch
    # tautologies that static analysis can't detect (e.g., complex expressions).
    # Only applies to WHERE clauses since they're the security-relevant filter.
    if db_check_tautology and where and tautology_check_project_id:
        from submodules.model.business_objects import general

        try:
            # Evaluate the condition for all rows in the project and check if
            # it always returns TRUE. We use DISTINCT to collapse results.
            # If we get exactly one row with value TRUE, it's an always-true condition.
            tautology_check_sql = f"""
                SELECT 
                    COUNT(DISTINCT d) = 1 
                    AND bool_or(d) = true 
                    AND COUNT(*) > 0 
                    AS is_tautology
                FROM (
                    SELECT ({where}) AS d 
                    FROM public.record 
                    WHERE project_id = '{tautology_check_project_id}'
                ) sub
            """
            result = general.execute_all(tautology_check_sql)
            if result and len(result) > 0 and result[0]["is_tautology"]:
                return "Database tautology check: WHERE condition evaluates to TRUE for all records in project"
        except Exception as e:
            # Don't fail validation if tautology check fails - it's an optional extra check
            general.rollback()
            # Log but don't block: the static checks are the primary defense
            pass

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


class _ConstantNotResolvable:
    """Sentinel class to indicate a value cannot be resolved to a constant."""

    pass


_NOT_RESOLVABLE = _ConstantNotResolvable()


def __get_constant_value(node):
    """
    Recursively evaluate a node if it only consists of literals and pure functions/operators.
    Returns _NOT_RESOLVABLE if the expression contains columns or cannot be evaluated.
    Returns None for SQL NULL values.
    Returns the actual value (int, float, str, bool) for constant expressions.
    """
    if node is None:
        return _NOT_RESOLVABLE

    # Unwrap parentheses
    if isinstance(node, exp.Paren):
        inner = node.args.get("this")
        return __get_constant_value(inner)

    # NULL literal
    if isinstance(node, exp.Null):
        return None  # Python None represents SQL NULL

    # Boolean literal
    if isinstance(node, exp.Boolean):
        return node.this  # True or False

    # Numeric or string literal
    if isinstance(node, exp.Literal):
        if node.is_number:
            try:
                # Try int first, then float
                val = node.this
                if "." in str(val):
                    return float(val)
                return int(val)
            except (ValueError, TypeError):
                return _NOT_RESOLVABLE
        elif node.is_string:
            return str(node.this)
        return _NOT_RESOLVABLE

    # Column or Identifier - not a constant
    if isinstance(node, (exp.Column, exp.Identifier)):
        return _NOT_RESOLVABLE

    # Binary arithmetic operations: +, -, *, /
    if isinstance(node, exp.Add):
        left = __get_constant_value(node.left)
        right = __get_constant_value(node.right)
        if isinstance(left, _ConstantNotResolvable) or isinstance(
            right, _ConstantNotResolvable
        ):
            return _NOT_RESOLVABLE
        if left is None or right is None:
            return None  # NULL propagation
        try:
            return left + right
        except Exception:
            return _NOT_RESOLVABLE

    if isinstance(node, exp.Sub):
        left = __get_constant_value(node.left)
        right = __get_constant_value(node.right)
        if isinstance(left, _ConstantNotResolvable) or isinstance(
            right, _ConstantNotResolvable
        ):
            return _NOT_RESOLVABLE
        if left is None or right is None:
            return None
        try:
            return left - right
        except Exception:
            return _NOT_RESOLVABLE

    if isinstance(node, exp.Mul):
        left = __get_constant_value(node.left)
        right = __get_constant_value(node.right)
        if isinstance(left, _ConstantNotResolvable) or isinstance(
            right, _ConstantNotResolvable
        ):
            return _NOT_RESOLVABLE
        if left is None or right is None:
            return None
        try:
            return left * right
        except Exception:
            return _NOT_RESOLVABLE

    if isinstance(node, exp.Div):
        left = __get_constant_value(node.left)
        right = __get_constant_value(node.right)
        if isinstance(left, _ConstantNotResolvable) or isinstance(
            right, _ConstantNotResolvable
        ):
            return _NOT_RESOLVABLE
        if left is None or right is None:
            return None
        try:
            if right == 0:
                return _NOT_RESOLVABLE  # Division by zero
            # Use integer division if both are ints
            if isinstance(left, int) and isinstance(right, int):
                return left // right if left % right == 0 else left / right
            return left / right
        except Exception:
            return _NOT_RESOLVABLE

    # COALESCE: return first non-null value
    if isinstance(node, exp.Coalesce):
        args = [node.this] + (node.expressions or [])
        for arg in args:
            val = __get_constant_value(arg)
            if isinstance(val, _ConstantNotResolvable):
                return _NOT_RESOLVABLE
            if val is not None:  # First non-null
                return val
        return None  # All were NULL

    # NULLIF(a, b): returns NULL if a = b, otherwise a
    if isinstance(node, exp.Nullif):
        a = __get_constant_value(node.this)
        b = __get_constant_value(node.expression)
        if isinstance(a, _ConstantNotResolvable) or isinstance(
            b, _ConstantNotResolvable
        ):
            return _NOT_RESOLVABLE
        if a == b:
            return None
        return a

    # ABS function
    if isinstance(node, exp.Abs):
        val = __get_constant_value(node.this)
        if isinstance(val, _ConstantNotResolvable):
            return _NOT_RESOLVABLE
        if val is None:
            return None
        try:
            return abs(val)
        except Exception:
            return _NOT_RESOLVABLE

    # Cast - try to evaluate the inner value
    if isinstance(node, exp.Cast):
        inner = __get_constant_value(node.this)
        if isinstance(inner, _ConstantNotResolvable):
            return _NOT_RESOLVABLE
        # For now, just return the inner value (type coercion not fully implemented)
        return inner

    # Neg (unary minus)
    if isinstance(node, exp.Neg):
        val = __get_constant_value(node.this)
        if isinstance(val, _ConstantNotResolvable):
            return _NOT_RESOLVABLE
        if val is None:
            return None
        try:
            return -val
        except Exception:
            return _NOT_RESOLVABLE

    # For any other node type, we can't resolve it
    return _NOT_RESOLVABLE


def __is_constant_comparison_true(expr):
    """
    Check if an EQ expression evaluates to true using constant evaluation.
    Returns a reason string if it's a constant true comparison, None otherwise.
    """
    if not isinstance(expr, exp.EQ):
        return None

    left = expr.args.get("this")
    right = expr.args.get("expression")

    left_val = __get_constant_value(left)
    right_val = __get_constant_value(right)

    # Both must be resolvable constants (not _NOT_RESOLVABLE)
    if isinstance(left_val, _ConstantNotResolvable) or isinstance(
        right_val, _ConstantNotResolvable
    ):
        return None

    # NULL = NULL is NULL in SQL, not true - so we don't flag it
    if left_val is None or right_val is None:
        return None

    # Compare the values
    try:
        # Handle numeric comparison with tolerance for floats
        if isinstance(left_val, (int, float)) and isinstance(right_val, (int, float)):
            if abs(left_val - right_val) < 1e-9:
                return f"Constant comparison evaluates to true: {left} = {right}"
        elif left_val == right_val:
            return f"Constant comparison evaluates to true: {left} = {right}"
    except Exception:
        pass

    return None


def __is_constant_is_null_true(expr):
    """
    Check if an IS NULL expression evaluates to true using constant evaluation.
    Returns a reason string if it's a constant true IS NULL, None otherwise.
    """
    if not isinstance(expr, exp.Is):
        return None

    left = expr.args.get("this")
    right = expr.args.get("expression")

    # Check for IS NULL pattern
    if isinstance(right, exp.Null):
        left_val = __get_constant_value(left)
        if isinstance(left_val, _ConstantNotResolvable):
            return None
        if left_val is None:
            return f"Constant IS NULL evaluates to true: {left} IS NULL"

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

    # NOT: Check for NOT FALSE (= TRUE) or NOT TRUE (= FALSE)
    # These are constant expressions that can bypass WHERE filters
    if isinstance(expr, exp.Not):
        inner = expr.args.get("this")
        if isinstance(inner, exp.Boolean):
            return f"Constant NOT expression: NOT {inner.this}"
        # Recurse into NOT to catch nested constants
        if isinstance(inner, exp.Expression):
            if reason := __contains_always_true(inner):
                return f"Constant in NOT expression: {reason}"

    # EQ: Check using constant evaluation (handles 10/2=5, coalesce(1,2)=1, etc.)
    if isinstance(expr, exp.EQ):
        if reason := __is_constant_comparison_true(expr):
            return reason
        # Also check for literal = literal as a fallback
        left = expr.args.get("this")
        right = expr.args.get("expression")
        if isinstance(left, exp.Literal) and isinstance(right, exp.Literal):
            if left.this == right.this:
                return f"Always-true expression: {left.this} = {right.this}"

    # Other comparison operators: <, >, <=, >=, != with constants
    # These can be tautologies like 1 < 2 or 1 != 2
    if isinstance(expr, (exp.LT, exp.GT, exp.LTE, exp.GTE, exp.NEQ)):
        left_val = __get_constant_value(expr.left)
        right_val = __get_constant_value(expr.right)
        # Both must be resolvable constants
        if not isinstance(left_val, _ConstantNotResolvable) and not isinstance(right_val, _ConstantNotResolvable):
            if left_val is not None and right_val is not None:
                try:
                    # Evaluate the comparison
                    if isinstance(expr, exp.LT) and left_val < right_val:
                        return f"Constant comparison tautology: {left_val} < {right_val}"
                    if isinstance(expr, exp.GT) and left_val > right_val:
                        return f"Constant comparison tautology: {left_val} > {right_val}"
                    if isinstance(expr, exp.LTE) and left_val <= right_val:
                        return f"Constant comparison tautology: {left_val} <= {right_val}"
                    if isinstance(expr, exp.GTE) and left_val >= right_val:
                        return f"Constant comparison tautology: {left_val} >= {right_val}"
                    if isinstance(expr, exp.NEQ) and left_val != right_val:
                        return f"Constant comparison tautology: {left_val} != {right_val}"
                except Exception:
                    pass

    # IS NULL: Check using constant evaluation (handles nullif(1,1) is null)
    if isinstance(expr, exp.Is):
        if reason := __is_constant_is_null_true(expr):
            return reason

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

    # AND: check both sides for tautologies
    # An AND with a tautology like (data->>'a' = 'b' AND TRUE) still has a constant
    if isinstance(expr, exp.And):
        left = expr.args.get("this")
        right = expr.args.get("expression")
        if left and isinstance(left, exp.Expression):
            if reason := __contains_always_true(left):
                return reason
        if right and isinstance(right, exp.Expression):
            if reason := __contains_always_true(right):
                return reason

    # OR: recurse to catch (1=1 OR ...) - CRITICAL: OR with tautology bypasses filters
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
        # COALESCE arguments are expr.this and expr.expressions
        args = [expr.this] + (expr.expressions or [])
        for arg in args:
            if not arg:
                continue
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

    # CASE: check if any branch condition is always true
    if isinstance(expr, exp.Case):
        for if_node in expr.args.get("ifs", []):
            if reason := __contains_always_true(if_node.this):
                return f"Constant CASE condition: {reason}"

    return None


def __get_max_expression_depth(node, current_depth=0) -> int:
    """
    Calculate the maximum nesting depth of an expression tree.
    Used to prevent DoS attacks via deeply nested expressions.
    """
    if not isinstance(node, exp.Expression):
        return current_depth

    max_child_depth = current_depth
    for child in node.iter_expressions():
        child_depth = __get_max_expression_depth(child, current_depth + 1)
        max_child_depth = max(max_child_depth, child_depth)

    return max_child_depth


def __check_regex_complexity(parsed) -> str | None:
    """
    Check for potentially dangerous regex patterns that could cause ReDoS.
    Returns a reason string if dangerous, None otherwise.
    """
    for node in parsed.walk():
        # Check RegexpLike and RegexpILike nodes (for ~ and ~* operators)
        if isinstance(node, (exp.RegexpLike, exp.RegexpILike)):
            pattern_node = node.expression
            if isinstance(pattern_node, exp.Literal) and pattern_node.is_string:
                pattern = pattern_node.this

                # Check pattern length
                if len(pattern) > MAX_REGEX_LENGTH:
                    return f"Regex pattern too long: {len(pattern)} characters (maximum allowed: {MAX_REGEX_LENGTH})"

                # Check for dangerous patterns that could cause catastrophic backtracking
                for dangerous in DANGEROUS_REGEX_PATTERNS:
                    if re.search(dangerous, pattern):
                        return f"Potentially dangerous regex pattern detected (could cause performance issues): {pattern[:50]}..."

                # Check for excessive quantifier nesting like (a+)+ or (a*)* 
                # These can cause exponential backtracking
                nested_quantifier_pattern = r'\([^)]*[+*][^)]*\)[+*]'
                if re.search(nested_quantifier_pattern, pattern):
                    return f"Nested quantifiers in regex pattern could cause performance issues: {pattern[:50]}..."

                # Check for excessive alternation with overlap
                # e.g., (a|a|a|a|a|a|a|a|a|a) - many similar alternatives
                if pattern.count('|') > 20:
                    return f"Too many alternatives in regex pattern: {pattern.count('|')} (maximum recommended: 20)"

    return None
