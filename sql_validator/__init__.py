"""
Restricted SQL validation submodule.

This package depends on `sqlglot` and may only be used in the validation environment.
Do NOT use or install `sqlglot` in other systems.
"""

# Try/except guard ensures clear failure if sqlglot is missing
try:
    from .sql_validator import validate_sql_clause
except ImportError as e:
    raise ImportError(
        "The `sql_validator` package requires `sqlglot` and is restricted "
        "to the validation environment. Do NOT install or import it elsewhere."
    ) from e

# Only expose this function at the package level
__all__ = ["validate_sql_clause"]
