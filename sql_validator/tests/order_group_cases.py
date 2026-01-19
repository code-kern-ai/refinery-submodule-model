VALID_CASES = [
    # --- Valid ORDER BY ---
    {"order_by": "data->>'name'"},
    {"order_by": "data->>'age' DESC"},
    {"order_by": "data->>'created_at' ASC NULLS LAST"},
    {"order_by": "data->'meta'->>'priority', data->>'name'"},
    {"order_by": "(data->>'score')::float DESC"},
    {"order_by": "lower(data->>'name')"},
    
    # --- Valid GROUP BY ---
    {"group_by": "data->>'category'"},
    {"group_by": "data->'tags'"},
    {"group_by": "data->>'year', data->>'month'"},
    {"group_by": "date_trunc('month', (data->>'joined_at')::timestamp)"},
    {"order_by": "COALESCE(NULL, data->>'name')"},
    {"order_by": "1"}, # Column index
    {"group_by": "1"}, # Column index
    {"order_by": "1 DESC, 2 ASC"},
    {"group_by": "1, 2, 3"},
]

INVALID_CASES = [
    # --- ORDER BY Injections ---
    {"order_by": "true"},
    {"order_by": "(SELECT 1)"}, # Subquery
    {"order_by": "(SELECT MAX(id) FROM record)"},
    {"order_by": "CASE WHEN (1=1) THEN data->>'name' ELSE data->>'age' END"}, # Tautology in CASE
    {"order_by": "CASE WHEN (data->>'role'='admin') THEN (SELECT pg_sleep(5)) ELSE data->>'name' END"},
    {"order_by": "data->>'name' -- comment"},
    {"order_by": "data->>'name'; DROP TABLE users"},
    {"order_by": "pg_sleep(5)"},
    {"order_by": "1/0"},
    {"order_by": "CAST(version() AS int)"},
    
    # --- GROUP BY Injections ---
    {"group_by": "(SELECT 1)"},
    {"group_by": "data->>'name' || (SELECT ' ') || data->>'role'"},
    {"group_by": "data->>'name'; DELETE FROM record"},
    {"group_by": "pg_sleep(2)"},
    
    # --- Variety of payloads ---
    {"order_by": "random()"},
    {"order_by": "version()"},
    {"order_by": "current_user"},
    {"order_by": "session_user"},
    {"order_by": "get_pg_database_settings()"},
    
    # --- Logical Variations ---
    {"order_by": "data->>'a' = 'b' OR 1=1"}, # Tautology
    {"order_by": "COALESCE(TRUE, (data->>'a'='b'))"}, # Tautology
]
