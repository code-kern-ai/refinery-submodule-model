"""
Safe SQLAlchemy helpers for JSONB/JSON column filtering without raw text().

Use these instead of sqlalchemy.text() with f-strings to avoid SQL injection
and satisfy avoid-sqlalchemy-text. Values are bound as parameters; only
fixed key names are embedded in the SQL structure.
"""

from typing import Any, List

from sqlalchemy.sql.elements import ColumnElement


def jsonb_key_equals(column: ColumnElement, key: str, value: Any) -> ColumnElement:
    """
    Build a filter clause: (column ->> key) = value.
    The key is a fixed identifier; value is bound as a parameter.
    """
    return column.op("->>")(key) == value


def jsonb_nested_key_equals(
    column: ColumnElement, outer_key: str, inner_key: str, value: Any
) -> ColumnElement:
    """
    Build a filter clause: (column -> outer_key ->> inner_key) = value.
    Used for JSON paths like task_info->'tmp_doc_metadata'->>'conversation_id'.
    Keys are fixed; value is bound as a parameter.
    """
    return column.op("->")(outer_key).op("->>")(inner_key) == value


def jsonb_key_in(column: ColumnElement, key: str, values: List[Any]) -> ColumnElement:
    """
    Build a filter clause: (column ->> key) IN (values).
    The key is fixed; values are bound as parameters.
    """
    return column.op("->>")(key).in_(values)
