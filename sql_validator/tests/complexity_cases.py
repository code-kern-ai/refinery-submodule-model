# Complexity limit and advanced feature test cases

VALID_CASES = [
    # --- DISTINCT ON (PostgreSQL-specific) ---
    {"select": "DISTINCT ON (data->>'category') data->>'name', data->>'category'"},
    {"select": "DISTINCT ON (data->>'type', data->>'status') data->>'name'"},
    {"select": "DISTINCT ON (data->>'group') data->>'name', data->>'value'"},
    {
        "select": "DISTINCT ON (data->>'category') data->>'name', data->>'created_at'",
        "order_by": "data->>'category', data->>'created_at' DESC"
    },
    
    # --- JSON path functions ---
    {"where": "jsonb_path_exists(data, '$.items[*]')"},
    {"where": "jsonb_path_exists(data, '$.name')"},
    {"where": "jsonb_path_exists(data, '$.items[*] ? (@.price > 10)')"},
    {"where": "jsonb_path_match(data, '$.active')"},
    {"select": "jsonb_path_query(data, '$.items[*].name')"},
    {"select": "jsonb_path_query_array(data, '$.tags[*]')"},
    {"select": "jsonb_path_query_first(data, '$.items[0]')"},
    {"where": "jsonb_path_exists(data, '$.metadata.tags[*] ? (@ == \"important\")')"},
    
    # --- Simple regex patterns ---
    {"where": "data->>'name' ~ '^[a-zA-Z]+$'"},
    {"where": "data->>'email' ~ '^[a-z]+@[a-z]+\\.[a-z]+$'"},
    {"where": "data->>'code' ~ '^[A-Z]{2,4}-[0-9]{4}$'"},
    {"where": "data->>'phone' ~* '^\\+?[0-9\\-\\s]+$'"},
    {"where": "data->>'name' !~ '^admin'"},
    {"where": "data->>'status' !~* '^(deleted|archived)$'"},
    
    # --- Reasonable expression depth ---
    {"where": "((data->>'a')::int + (data->>'b')::int) * ((data->>'c')::int - (data->>'d')::int) > 0"},
    {"where": "CASE WHEN (data->>'type') = 'A' THEN (data->>'val1')::int WHEN (data->>'type') = 'B' THEN (data->>'val2')::int ELSE 0 END > 10"},
    
    # --- Reasonable query length ---
    {"where": "data->>'field1' = 'value1' AND data->>'field2' = 'value2' AND data->>'field3' = 'value3'"},
    
    # --- Combined: DISTINCT ON with window functions ---
    {
        "select": "DISTINCT ON (data->>'category') data->>'name', ROW_NUMBER() OVER (PARTITION BY data->>'category' ORDER BY data->>'date' DESC)",
        "order_by": "data->>'category', data->>'date' DESC"
    },
]

INVALID_CASES = [
    # --- Query too long ---
    {"where": "data->>'name' = '" + "a" * 10001 + "'"},  # Over 10000 chars
    
    # --- Dangerous regex patterns (ReDoS) ---
    {"where": "data->>'name' ~ '^(a+)+$'"},  # Nested quantifiers
    {"where": "data->>'name' ~ '^(a*)*$'"},  # Nested quantifiers
    {"where": "data->>'name' ~ '^(a|a)+$'"},  # Overlapping alternatives with quantifier
    {"where": "data->>'name' ~ '^([a-z]+)+$'"},  # Nested quantifiers with character class
    
    # --- Regex too long ---
    {"where": "data->>'name' ~ '^" + "[a-z]" * 51 + "$'"},  # Over 200 chars (51*4 + 2 = 206)
    
    # --- Too many alternatives ---
    {"where": "data->>'name' ~ '^(" + "|".join([f"opt{i}" for i in range(25)]) + ")$'"},  # 25 alternatives
    
    # --- JSON path with subquery attempt (blocked by sub-select check) ---
    {"where": "jsonb_path_exists(data, (SELECT '$.name'))"},
    
    # --- JSON path with SQL keywords in path string (defense-in-depth) ---
    {"where": "jsonb_path_exists(data, '$.items[*] ? (@.id == (SELECT 1))')"},
    {"where": "jsonb_path_query(data, '$.x ? (@ == (SELECT MAX(id) FROM users))')"},
    {"where": "jsonb_path_exists(data, '$.items WHERE true')"},
    {"where": "jsonb_path_query_array(data, '$.x UNION $.y')"},
    {"where": "jsonb_path_query_first(data, '$.items DELETE')"},
    {"where": "jsonb_path_match(data, '$.x INSERT into')"},
    {"where": "jsonb_path_exists(data, '$.data FROM table')"},
    {"where": "jsonb_path_query(data, '$.items JOIN other')"},
    
    # --- DISTINCT ON with non-data column ---
    {"select": "DISTINCT ON (id) data->>'name'"},
    {"select": "DISTINCT ON (created_at) data->>'name'"},
    {"select": "DISTINCT ON (data->>'cat', id) data->>'name'"},
    
    # --- DISTINCT ON with subquery ---
    {"select": "DISTINCT ON ((SELECT 1)) data->>'name'"},
    
    # --- Regex with system function ---
    {"where": "version() ~ '^PostgreSQL'"},
    {"where": "current_user ~ '^admin'"},
]
