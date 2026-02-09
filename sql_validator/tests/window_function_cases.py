# Window function test cases
# Window functions operate on the result set AFTER WHERE filtering,
# so they cannot access data outside the project_id filter.

VALID_CASES = [
    # --- Basic window functions ---
    {"select": "data->>'name', ROW_NUMBER() OVER (PARTITION BY data->>'category' ORDER BY data->>'id' ASC) AS rn"},
    {"select": "ROW_NUMBER() OVER (ORDER BY data->>'date' DESC)"},
    {"select": "RANK() OVER (ORDER BY (data->>'score')::int DESC)"},
    {"select": "DENSE_RANK() OVER (PARTITION BY data->>'type' ORDER BY data->>'value')"},
    {"select": "NTILE(4) OVER (ORDER BY (data->>'score')::int)"},
    {"select": "PERCENT_RANK() OVER (ORDER BY (data->>'score')::int)"},
    {"select": "CUME_DIST() OVER (ORDER BY (data->>'score')::int)"},
    
    # --- LAG/LEAD functions ---
    {"select": "LAG(data->>'value') OVER (ORDER BY data->>'seq')"},
    {"select": "LAG(data->>'value', 1) OVER (ORDER BY data->>'seq')"},
    {"select": "LAG(data->>'value', 1, 'default') OVER (ORDER BY data->>'seq')"},
    {"select": "LEAD(data->>'value') OVER (ORDER BY data->>'seq')"},
    {"select": "LEAD(data->>'value', 2, 'N/A') OVER (ORDER BY data->>'seq')"},
    
    # --- FIRST_VALUE/LAST_VALUE/NTH_VALUE ---
    {"select": "FIRST_VALUE(data->>'name') OVER (PARTITION BY data->>'group' ORDER BY data->>'rank')"},
    {"select": "LAST_VALUE(data->>'name') OVER (PARTITION BY data->>'group' ORDER BY data->>'rank')"},
    {"select": "NTH_VALUE(data->>'name', 2) OVER (ORDER BY data->>'rank')"},
    
    # --- Aggregate window functions ---
    {"select": "SUM((data->>'amount')::numeric) OVER (PARTITION BY data->>'category')"},
    {"select": "COUNT(1) OVER (PARTITION BY data->>'type')"},
    {"select": "AVG((data->>'value')::numeric) OVER ()"},
    {"select": "MIN((data->>'value')::numeric) OVER (ORDER BY data->>'date')"},
    {"select": "MAX((data->>'value')::numeric) OVER (ORDER BY data->>'date')"},
    
    # --- With frame specifications ---
    {"select": "SUM((data->>'amt')::numeric) OVER (ORDER BY data->>'date' ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"},
    {"select": "AVG((data->>'val')::numeric) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"},
    {"select": "SUM((data->>'amt')::numeric) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"},
    {"select": "COUNT(1) OVER (ORDER BY data->>'date' ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING)"},
    
    # --- Multiple window functions (within limit) ---
    {"select": "data->>'name', ROW_NUMBER() OVER (ORDER BY data->>'id'), RANK() OVER (ORDER BY (data->>'score')::int)"},
    {"select": "ROW_NUMBER() OVER (ORDER BY data->>'a'), ROW_NUMBER() OVER (ORDER BY data->>'b'), ROW_NUMBER() OVER (ORDER BY data->>'c')"},
    
    # --- Window function with complex expressions ---
    {"select": "data->>'category', SUM(CASE WHEN (data->>'status')='active' THEN 1 ELSE 0 END) OVER (PARTITION BY data->>'category')"},
    {"select": "data->>'name', (data->>'score')::int, AVG((data->>'score')::int) OVER (PARTITION BY data->>'group')"},
    
    # --- Combined with column index ORDER BY ---
    {
        "select": "data->>'name', ROW_NUMBER() OVER (PARTITION BY data->>'category' ORDER BY data->>'id' ASC) AS rn",
        "order_by": "1, 2"
    },
    {
        "select": "data->>'category', SUM((data->>'amount')::numeric) OVER (PARTITION BY data->>'category')",
        "where": "data->>'status' = 'active'",
        "order_by": "1"
    },
    
    # --- Window functions with random() (allowed) ---
    {"select": "ROW_NUMBER() OVER (ORDER BY random())"},
    {"select": "data->>'name', ROW_NUMBER() OVER (ORDER BY random()) AS random_order"},
    
    # --- Exactly at the complexity limit (5 window functions) ---
    {"select": "ROW_NUMBER() OVER (ORDER BY data->>'a'), ROW_NUMBER() OVER (ORDER BY data->>'b'), ROW_NUMBER() OVER (ORDER BY data->>'c'), ROW_NUMBER() OVER (ORDER BY data->>'d'), ROW_NUMBER() OVER (ORDER BY data->>'e')"},
]

INVALID_CASES = [
    # --- Subquery attacks in window functions ---
    {"select": "ROW_NUMBER() OVER (PARTITION BY (SELECT 1))"},
    {"select": "ROW_NUMBER() OVER (PARTITION BY (SELECT data FROM record LIMIT 1))"},
    {"select": "ROW_NUMBER() OVER (ORDER BY (SELECT MAX(id) FROM record))"},
    {"select": "ROW_NUMBER() OVER (ORDER BY (SELECT 1 FROM record WHERE project_id != 'x' LIMIT 1))"},
    {"select": "LAG((SELECT data FROM record LIMIT 1)) OVER ()"},
    {"select": "LEAD((SELECT 1)) OVER (ORDER BY data->>'seq')"},
    {"select": "FIRST_VALUE((SELECT data->>'name' FROM record LIMIT 1)) OVER ()"},
    {"select": "SUM((SELECT 1)) OVER ()"},
    
    # --- Non-data column access in window functions ---
    {"select": "ROW_NUMBER() OVER (PARTITION BY id)"},
    {"select": "ROW_NUMBER() OVER (PARTITION BY project_id)"},
    {"select": "ROW_NUMBER() OVER (PARTITION BY created_at)"},
    {"select": "ROW_NUMBER() OVER (ORDER BY id)"},
    {"select": "ROW_NUMBER() OVER (ORDER BY created_at)"},
    {"select": "ROW_NUMBER() OVER (ORDER BY id DESC)"},
    {"select": "LAG(id) OVER (ORDER BY data->>'seq')"},
    {"select": "LEAD(created_at) OVER ()"},
    {"select": "FIRST_VALUE(id) OVER ()"},
    {"select": "FIRST_VALUE(created_at) OVER ()"},
    {"select": "SUM(id) OVER ()"},
    {"select": "AVG(id) OVER (PARTITION BY data->>'category')"},
    
    # --- System function attacks in window functions ---
    {"select": "ROW_NUMBER() OVER (ORDER BY pg_sleep(1))"},
    {"select": "ROW_NUMBER() OVER (ORDER BY version())"},
    {"select": "ROW_NUMBER() OVER (PARTITION BY current_user)"},
    {"select": "ROW_NUMBER() OVER (ORDER BY inet_client_addr())"},
    {"select": "LAG(version()) OVER ()"},
    {"select": "FIRST_VALUE(current_user) OVER ()"},
    
    # --- Star (*) in window aggregates ---
    {"select": "COUNT(*) OVER ()"},
    {"select": "SUM(*) OVER ()"},
    {"select": "COUNT(*) OVER (PARTITION BY data->>'category')"},
    {"select": "AVG(*) OVER ()"},
    {"select": "MIN(*) OVER ()"},
    {"select": "MAX(*) OVER ()"},
    
    # --- Tautology in window ORDER BY ---
    {"select": "ROW_NUMBER() OVER (ORDER BY 1=1)"},
    {"select": "ROW_NUMBER() OVER (ORDER BY CASE WHEN 1=1 THEN 1 ELSE 0 END)"},
    {"select": "RANK() OVER (ORDER BY TRUE)"},
    
    # --- FILTER clause (not supported) ---
    {"select": "SUM(1) FILTER (WHERE data->>'status'='active') OVER ()"},
    {"select": "COUNT(1) FILTER (WHERE 1=1) OVER ()"},
    {"select": "AVG((data->>'value')::numeric) FILTER (WHERE data->>'type'='A') OVER ()"},
    
    # --- Complexity limit: exceeding MAX_WINDOW_FUNCTIONS (5) ---
    {"select": "ROW_NUMBER() OVER (ORDER BY data->>'a'), ROW_NUMBER() OVER (ORDER BY data->>'b'), ROW_NUMBER() OVER (ORDER BY data->>'c'), ROW_NUMBER() OVER (ORDER BY data->>'d'), ROW_NUMBER() OVER (ORDER BY data->>'e'), ROW_NUMBER() OVER (ORDER BY data->>'f')"},
    {"select": "ROW_NUMBER() OVER (), RANK() OVER (), DENSE_RANK() OVER (), NTILE(4) OVER (), LAG(data->>'x') OVER (), LEAD(data->>'x') OVER ()"},
    {"select": "SUM(1) OVER (), COUNT(1) OVER (), AVG(1) OVER (), MIN(1) OVER (), MAX(1) OVER (), ROW_NUMBER() OVER ()"},
    # 10 window functions - way over limit
    {"select": "ROW_NUMBER() OVER (), ROW_NUMBER() OVER (), ROW_NUMBER() OVER (), ROW_NUMBER() OVER (), ROW_NUMBER() OVER (), ROW_NUMBER() OVER (), ROW_NUMBER() OVER (), ROW_NUMBER() OVER (), ROW_NUMBER() OVER (), ROW_NUMBER() OVER ()"},
    
    # --- Mixed attacks: valid window syntax but dangerous content ---
    {"select": "data->>'name', ROW_NUMBER() OVER (PARTITION BY id ORDER BY data->>'date')"},  # non-data in PARTITION
    {"select": "data->>'name', ROW_NUMBER() OVER (PARTITION BY data->>'cat' ORDER BY id)"},  # non-data in ORDER
    {"select": "data->>'name', LAG(id) OVER (ORDER BY data->>'date')"},  # non-data in LAG argument
]
