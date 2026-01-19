AGG_EXPR = "data->>'name'"

VALID_CASES = [
    # --- Complex CASE in SELECT ---
    {
        "select": "CASE WHEN (data->>'role') = 'admin' THEN 'A' WHEN (data->>'role') = 'user' THEN 'U' ELSE 'O' END",
        "where": "data->>'id' IS NOT NULL"
    },
    # --- Complex CASE in GROUP BY ---
    {
        "select": "CASE WHEN (data->>'age')::int > 18 THEN 'adult' ELSE 'minor' END, count(1)",
        "group_by": "CASE WHEN (data->>'age')::int > 18 THEN 'adult' ELSE 'minor' END"
    },
    # --- Complex CASE in ORDER BY ---
    {
        "select": "data->>'name'",
        "order_by": "CASE WHEN (data->>'priority')::int = 1 THEN 0 ELSE 1 END, data->>'name' ASC"
    },
    # --- Full multi-part valid query ---
    {
        "select": "data->>'category', sum((data->>'amount')::numeric)",
        "where": "(data->>'date')::date > '2023-01-01'",
        "group_by": "data->>'category'",
        "order_by": "sum((data->>'amount')::numeric) DESC"
    },
    # --- CASE in SELECT with multiple WHEN clauses ---
    {
        "select": "CASE WHEN (data->>'score')::int >= 90 THEN 'A' WHEN (data->>'score')::int >= 80 THEN 'B' WHEN (data->>'score')::int >= 70 THEN 'C' ELSE 'F' END",
        "where": "data->>'score' IS NOT NULL"
    },
    # --- CASE in ORDER BY with nested conditions ---
    {
        "select": "data->>'name', (data->>'priority')::int",
        "order_by": "CASE WHEN (data->>'priority')::int = 1 THEN 0 WHEN (data->>'priority')::int = 2 THEN 1 ELSE 2 END, data->>'name'"
    },
    # --- CASE in GROUP BY with complex expression ---
    {
        "select": "CASE WHEN (data->>'age')::int BETWEEN 18 AND 65 THEN 'working' WHEN (data->>'age')::int < 18 THEN 'minor' ELSE 'senior' END, count(1)",
        "group_by": "CASE WHEN (data->>'age')::int BETWEEN 18 AND 65 THEN 'working' WHEN (data->>'age')::int < 18 THEN 'minor' ELSE 'senior' END"
    },
    # --- Multiple CASE statements in one query ---
    {
        "select": "CASE WHEN (data->>'status') = 'active' THEN 'A' ELSE 'I' END, CASE WHEN (data->>'verified')::boolean THEN 'V' ELSE 'U' END",
        "where": "data->>'id' IS NOT NULL",
        "order_by": "CASE WHEN (data->>'status') = 'active' THEN 0 ELSE 1 END"
    },
    # --- CASE with valid literal comparisons (not tautologies) ---
    {
        "select": "CASE WHEN (data->>'a'='a') THEN data->>'a' ELSE data->>'b' END"
    },
    # --- JSONB nesting and containment ---
    {
        "select": "data->'meta'->'tags' || '[\"processed\"]'::jsonb",
        "where": "data->'meta'->'tags' @> '[\"urgent\"]'::jsonb AND NOT (data->'meta'->>'status' = 'done')"
    },
    # --- Distinct and aggregates ---
    {
        "select": "count(DISTINCT data->>'user_id')",
        "where": "data->>'action' = 'login'"
    },
    # --- String manipulation mix ---
    {
        "select": "upper(trim(data->>'first_name')) || ' ' || upper(trim(data->>'last_name'))",
        "where": "length(data->>'bio') > 100"
    },
    # --- Complex aggregates with expressions ---
    {
        "select": "data->>'category', sum((data->>'price')::numeric * (data->>'quantity')::int)",
        "group_by": "data->>'category'"
    },
    {
        "select": "data->>'department', avg((data->>'salary')::numeric), min((data->>'salary')::numeric), max((data->>'salary')::numeric)",
        "group_by": "data->>'department'"
    },
    # --- NULL handling in aggregates ---
    {
        "select": "data->>'category', count(1), count(data->>'name')",
        "group_by": "data->>'category'"
    },
    # --- Multiple ORDER BY columns ---
    {
        "select": "data->>'name', (data->>'score')::int",
        "order_by": "(data->>'score')::int DESC NULLS LAST, data->>'name' ASC"
    },
    # --- GROUP BY with multiple columns ---
    {
        "select": "data->>'year', data->>'month', count(1)",
        "group_by": "data->>'year', data->>'month'"
    },
    # --- HAVING clause equivalent via WHERE on aggregates (if supported) ---
    # Note: HAVING would require a different validation path, but we can test complex WHERE
    {
        "select": "data->>'category', count(1)",
        "where": "data->>'status' = 'active'",
        "group_by": "data->>'category'"
    },
    # --- Comprehensive edge cases: All parts pass (complex valid queries) ---
    {
        "select": "data->>'name', count(1)",
        "where": "data->>'role' = 'admin'",
        "group_by": "data->>'name'",
        "order_by": "count(1) DESC"
    },
    {
        "select": "data->>'category', sum((data->>'amount')::numeric), count(1)",
        "where": "(data->>'date')::date > '2023-01-01' AND data->>'status' = 'active'",
        "group_by": "data->>'category'",
        "order_by": "sum((data->>'amount')::numeric) DESC, data->>'category' ASC"
    },
    # --- More valid complex queries with all parts ---
    {
        "select": "data->>'category', sum((data->>'amount')::numeric), count(1), avg((data->>'price')::numeric)",
        "where": "(data->>'date')::date BETWEEN '2023-01-01' AND '2023-12-31' AND data->>'status' = 'active'",
        "group_by": "data->>'category'",
        "order_by": "sum((data->>'amount')::numeric) DESC, data->>'category' ASC"
    },
    {
        "select": "CASE WHEN (data->>'age')::int >= 18 THEN 'adult' ELSE 'minor' END, count(1)",
        "where": "(data->>'verified')::boolean = true AND (data->>'active')::boolean = true",
        "group_by": "CASE WHEN (data->>'age')::int >= 18 THEN 'adult' ELSE 'minor' END",
        "order_by": "count(1) DESC"
    },
    {
        "select": "data->>'department', count(DISTINCT data->>'employee_id'), sum((data->>'salary')::numeric)",
        "where": "(data->>'hire_date')::date > '2020-01-01'",
        "group_by": "data->>'department'",
        "order_by": "sum((data->>'salary')::numeric) DESC"
    },
    # --- Valid: CASE with multiple conditions ---
    {
        "select": "CASE WHEN (data->>'score')::int >= 90 THEN 'A' WHEN (data->>'score')::int >= 80 THEN 'B' WHEN (data->>'score')::int >= 70 THEN 'C' ELSE 'F' END, count(1)",
        "group_by": "CASE WHEN (data->>'score')::int >= 90 THEN 'A' WHEN (data->>'score')::int >= 80 THEN 'B' WHEN (data->>'score')::int >= 70 THEN 'C' ELSE 'F' END"
    },
    # --- Valid: Complex ORDER BY with multiple expressions ---
    {
        "select": "data->>'name', (data->>'score')::int, (data->>'priority')::int",
        "order_by": "(data->>'priority')::int DESC NULLS LAST, (data->>'score')::int DESC, data->>'name' ASC"
    },
    # --- Valid: GROUP BY with multiple columns and aggregates ---
    {
        "select": "data->>'year', data->>'month', count(1), sum((data->>'amount')::numeric), avg((data->>'amount')::numeric)",
        "group_by": "data->>'year', data->>'month'",
        "order_by": "data->>'year' DESC, data->>'month' DESC"
    },
    {
        "select": "data->>'category', count(1)",
        "group_by": "1",
        "order_by": "2 DESC"
    },
    {
        "select": "data->>'year', data->>'month', sum((data->>'amount')::numeric)",
        "group_by": "1, 2",
        "order_by": "1 DESC, 2 ASC"
    },
    # --- Valid: Complex nested CASE statements ---
    {
        "select": "CASE WHEN (data->>'status') = 'active' THEN CASE WHEN (data->>'verified')::boolean THEN 'AV' ELSE 'AU' END ELSE 'I' END",
        "where": "data->>'id' IS NOT NULL"
    },
    # --- Valid: Multiple aggregates with different functions ---
    {
        "select": "data->>'category', count(1), sum((data->>'qty')::int), avg((data->>'price')::numeric), min((data->>'price')::numeric), max((data->>'price')::numeric)",
        "group_by": "data->>'category'"
    },
    # --- Valid: ORDER BY with NULLS handling ---
    {
        "select": "data->>'name', (data->>'score')::int",
        "order_by": "data->>'name' ASC NULLS FIRST, (data->>'score')::int DESC NULLS LAST"
    },
    # --- Valid: Complex WHERE with multiple conditions ---
    {
        "select": "data->>'name'",
        "where": "(data->>'age')::int >= 18 AND (data->>'age')::int <= 65 AND (data->>'verified')::boolean = true AND data->>'status' = 'active'"
    },
    # --- Valid: JSONB array operations ---
    {
        "select": "data->>'name'",
        "where": "jsonb_array_length(data->'tags') > 0 AND data->'tags' @> '[\"premium\"]'::jsonb"
    },
    # --- Valid: Full text search with JSONB ---
    {
        "select": "data->>'title'",
        "where": "to_tsvector('english', data->>'description') @@ plainto_tsquery('search term')"
    },
]

INVALID_CASES = [
    # --- Tautology in CASE SELECT ---
    {
        "select": "CASE WHEN 1=1 THEN data->>'a' ELSE data->>'b' END",
    },
    # --- Subquery in CASE GROUP BY ---
    {
        "select": "count(*)",
        "group_by": "CASE WHEN (SELECT 1)=1 THEN data->>'a' ELSE data->>'b' END"
    },
    # --- Only one part fails (Subquery in WHERE) ---
    {
        "select": "data->>'name'",
        "where": "data->>'id' IN (SELECT id FROM record)",
        "order_by": "data->>'name'"
    },
    # --- Only one part fails (Tautology in ORDER BY) ---
    {
        "select": "data->>'name'",
        "where": "data->>'role' = 'admin'",
        "order_by": "1=1"
    },
    # --- Only one part fails (Disallowed column in SELECT) ---
    {
        "select": "id, data->>'name'",
        "where": "data->>'role' = 'admin'"
    },
    # --- Only one part fails (Disallowed function in GROUP BY) ---
    {
        "select": "data->>'category', count(*)",
        "group_by": "data->>'category', pg_sleep(0.1)"
    },
    # --- All parts fail ---
    {
        "select": "(SELECT version())",
        "where": "1=1",
        "group_by": "pg_sleep(1)",
        "order_by": "random()"
    },
    # --- Logical variation: Tautology via COALESCE in WHERE ---
    {
        "select": "data->>'name'",
        "where": "COALESCE(TRUE, (data->>'a'='b'))"
    },
    # --- Logical variation: Tautology via OR in SELECT ---
    {
        "select": "data->>'name', (data->>'role'='admin' OR 1=1)"
    },
    # --- Recursive structure: CASE with tautology deep inside ---
    {
        "select": "CASE WHEN data->>'a'='b' THEN (CASE WHEN 1=1 THEN 'x' ELSE 'y' END) ELSE 'z' END"
    },
    # --- Complex Postgres bypass attempt: LATERAL JOIN ---
    "data->>'a' = 'b' CROSS JOIN LATERAL (SELECT 1) AS t",
    # --- Complex Postgres bypass attempt: WINDOW functions ---
    "row_number() OVER (PARTITION BY data->>'category' ORDER BY data->>'name')",
    # --- Complex Postgres bypass attempt: CAST to system type ---
    "(data->>'id')::regclass",
    # --- Complex Postgres bypass attempt: ARRAY subquery ---
    "data->'tags' @> ARRAY(SELECT 'tag')",
    # --- SELECT * in various contexts (all must fail) ---
    {
        "select": "*",
        "where": "data->>'id' = '1'"
    },
    {
        "select": "data->>'name', *",
        "where": "data->>'id' = '1'"
    },
    {
        "select": "*",
        "group_by": "data->>'category'"
    },
    {
        "select": "*",
        "order_by": "data->>'name'"
    },
    {
        "select": "*",
        "where": "data->>'id' = '1'",
        "group_by": "data->>'category'",
        "order_by": "data->>'name'"
    },
    # --- Star in ORDER BY / GROUP BY (should fail) ---
    {
        "select": "data->>'name'",
        "order_by": "*"
    },
    {
        "select": "data->>'name'",
        "group_by": "*"
    },
    # --- Aggregates with * (ALL must fail - * is never allowed) ---
    {"select": "count(*)"},
    {"select": "count(*), data->>'name'"},
    {"select": "sum(*)"},
    {"select": "avg(*)"},
    {"select": "min(*)"},
    {"select": "max(*)"},
    {"select": "stddev(*)"},
    {"select": "variance(*)"},
    {"select": "count(*), sum((data->>'amount')::numeric)"},
    {
        "select": "count(*), data->>'category'",
        "group_by": "data->>'category'"
    },
    {
        "select": "CASE WHEN (data->>'age')::int > 18 THEN 'adult' ELSE 'minor' END, count(*)",
        "group_by": "CASE WHEN (data->>'age')::int > 18 THEN 'adult' ELSE 'minor' END"
    },
    # --- DISTINCT with * (should fail) ---
    {"select": "count(DISTINCT *)"},
    {"select": "sum(DISTINCT *)"},
    # --- Comprehensive edge cases: Only SELECT fails (subquery) ---
    {
        "select": "(SELECT MAX(id) FROM record)",
        "where": "data->>'role' = 'admin'",
        "group_by": "data->>'category'",
        "order_by": "data->>'name'"
    },
    # --- Comprehensive edge cases: Only WHERE fails (tautology) ---
    {
        "select": "data->>'name', count(1)",
        "where": "1=1",
        "group_by": "data->>'name'",
        "order_by": "data->>'name'"
    },
    # --- Comprehensive edge cases: Only GROUP BY fails (subquery) ---
    {
        "select": "data->>'category', count(1)",
        "where": "data->>'status' = 'active'",
        "group_by": "(SELECT 1)",
        "order_by": "count(1) DESC"
    },
    # --- Comprehensive edge cases: Only ORDER BY fails (subquery) ---
    {
        "select": "data->>'name', count(1)",
        "where": "data->>'role' = 'admin'",
        "group_by": "data->>'name'",
        "order_by": "(SELECT MAX(id) FROM record)"
    },
    # --- Comprehensive edge cases: SELECT and WHERE fail ---
    {
        "select": "(SELECT version())",
        "where": "1=1",
        "group_by": "data->>'category'",
        "order_by": "data->>'name'"
    },
    # --- Comprehensive edge cases: SELECT and GROUP BY fail ---
    {
        "select": "count(*)",
        "where": "data->>'status' = 'active'",
        "group_by": "pg_sleep(1)",
        "order_by": "data->>'name'"
    },
    # --- Comprehensive edge cases: WHERE and ORDER BY fail ---
    {
        "select": "data->>'name', count(1)",
        "where": "COALESCE(TRUE, (data->>'a'='b'))",
        "group_by": "data->>'name'",
        "order_by": "random()"
    },
    # --- Comprehensive edge cases: All parts fail ---
    {
        "select": "count(*)",
        "where": "1=1",
        "group_by": "pg_sleep(1)",
        "order_by": "version()"
    },
    {
        "select": "(SELECT MAX(id) FROM record)",
        "where": "data->>'a' = data->>'a'",
        "group_by": "(SELECT 1)",
        "order_by": "(SELECT pg_sleep(5))"
    },
    # --- Complex Postgres features that should fail ---
    "data->>'a' = 'b' WINDOW w AS (PARTITION BY data->>'category')",
    "data->>'a' = 'b' QUALIFY row_number() OVER (PARTITION BY data->>'category') = 1",
    "data->>'a' = 'b' FETCH FIRST 10 ROWS ONLY",
    "data->>'a' = 'b' FOR UPDATE",
    "data->>'a' = 'b' FOR SHARE",
    "data->>'a' = 'b' WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t",
    "data->>'a' = 'b' WITH t AS (SELECT 1) SELECT * FROM t",
    # --- More comprehensive SELECT * variations ---
    {"select": "*, data->>'name'"},
    {"select": "data->>'name', *, data->>'age'"},
    {"select": "t.*"},
    {"select": "alias.*"},
    {"select": "schema.table.*"},
    {"select": "public.record.*"},
    {"select": "r.*"},
    {"select": "record.*"},
    {"select": "data->>'name', (SELECT *)"},
    {"select": "data->>'name', * FROM (SELECT 1) AS t"},
    # --- Star in various aggregate contexts (all must fail) ---
    {"select": "sum(*)"},
    {"select": "avg(*)"},
    {"select": "min(*)"},
    {"select": "max(*)"},
    {"select": "stddev(*)"},
    {"select": "variance(*)"},
    {"select": "array_agg(*)"},
    {"select": "string_agg(*, ',')"},
    {"select": "json_agg(*)"},
    {"select": "jsonb_agg(*)"},
    {"select": "corr(*, *)"},
    {"select": "covar_pop(*, *)"},
    {"select": "regr_slope(*, *)"},
    {"select": "regr_intercept(*, *)"},
    {"select": "bool_and(*)"},
    {"select": "bool_or(*)"},
    {"select": "bit_and(*)"},
    {"select": "bit_or(*)"},
    {"select": "any_value(*)"},
    # --- Star in ORDER BY / GROUP BY variations ---
    {"order_by": "*, data->>'name'"},
    {"order_by": "data->>'name', *"},
    {"group_by": "*, data->>'category'"},
    {"group_by": "data->>'category', *"},
    {"order_by": "t.*"},
    {"group_by": "r.*"},
    # --- CASE with tautology in WHEN ---
    {
        "select": "CASE WHEN 1=1 THEN data->>'a' ELSE data->>'b' END"
    },
    {
        "select": "CASE WHEN TRUE THEN data->>'a' WHEN FALSE THEN data->>'b' ELSE data->>'c' END"
    },
    {
        "select": "CASE WHEN (data->>'a' = data->>'a') THEN data->>'a' ELSE data->>'b' END"
    },
    # --- CASE with subquery ---
    {
        "select": "CASE WHEN (SELECT 1)=1 THEN data->>'a' ELSE data->>'b' END"
    },
    {
        "order_by": "CASE WHEN (SELECT 1)=1 THEN data->>'name' ELSE data->>'age' END"
    },
    {
        "group_by": "CASE WHEN EXISTS(SELECT 1) THEN data->>'a' ELSE data->>'b' END"
    },
    # --- Nested CASE statements with issues ---
    {
        "select": "CASE WHEN data->>'a'='b' THEN (CASE WHEN 1=1 THEN 'x' ELSE 'y' END) ELSE 'z' END"
    },
    {
        "select": "CASE WHEN data->>'a'='b' THEN (CASE WHEN (SELECT 1)=1 THEN 'x' ELSE 'y' END) ELSE 'z' END"
    },
    # --- Aggregate functions with invalid arguments ---
    {"select": "sum(*)"},
    {"select": "avg(*)"},
    {"select": "min(*)"},
    {"select": "max(*)"},
    {"select": "stddev(*)"},
    {"select": "variance(*)"},
    {"select": "array_agg(*)"},
    {"select": "string_agg(*, ',')"},
    {"select": "json_agg(*)"},
    {"select": "jsonb_agg(*)"},
    # --- DISTINCT with * in various aggregates ---
    {"select": "count(DISTINCT *)"},
    {"select": "sum(DISTINCT *)"},
    {"select": "array_agg(DISTINCT *)"},
    # --- Window functions (should fail - not in ALLOWED_NODES) ---
    {"select": "row_number() OVER ()"},
    {"select": "rank() OVER (PARTITION BY data->>'category')"},
    {"select": "dense_rank() OVER ()"},
    {"select": "lag(data->>'value') OVER ()"},
    {"select": "lead(data->>'value') OVER ()"},
    {"select": "first_value(data->>'value') OVER ()"},
    {"select": "last_value(data->>'value') OVER ()"},
    # --- CTE attempts (should fail) ---
    "WITH t AS (SELECT data->>'name') SELECT * FROM t",
    "WITH RECURSIVE t AS (SELECT 1) SELECT data->>'name' FROM t",
    # --- LATERAL joins (should fail) ---
    "data->>'a' = 'b' CROSS JOIN LATERAL (SELECT 1) AS t",
    "data->>'a' = 'b' JOIN LATERAL (SELECT data->>'name') AS t ON TRUE",
    # --- System catalog access attempts ---
    "data->>'a' = (SELECT tablename FROM pg_tables LIMIT 1)",
    "data->>'a' = (SELECT column_name FROM information_schema.columns LIMIT 1)",
    "data->>'a' = (SELECT schemaname FROM pg_namespace LIMIT 1)",
    # --- Type casting to system types (should be restricted) ---
    "(data->>'id')::regclass",
    "(data->>'id')::oid",
    "(data->>'id')::xid",
    "(data->>'id')::tid",
    # --- Array operations with subqueries ---
    "data->'tags' @> ARRAY(SELECT 'tag')",
    "data->'tags' = ARRAY(SELECT 'tag')",
    "ARRAY(SELECT id FROM record) && data->'tags'",
    # --- JSONB path operations with subqueries ---
    "jsonb_path_exists(data, '$.items[*] ? (@.id == (SELECT 1))')",
    "jsonb_path_query(data, '$.items[*] ? (@.value > (SELECT 1))')",
    # --- More comprehensive multi-clause failure combinations ---
    # SELECT fails (subquery), others pass
    {
        "select": "(SELECT MAX(id) FROM record)",
        "where": "data->>'status' = 'active'",
        "group_by": "data->>'category'",
        "order_by": "data->>'name'"
    },
    # WHERE fails (tautology), others pass
    {
        "select": "data->>'name', count(1)",
        "where": "data->>'a' = data->>'a'",
        "group_by": "data->>'name'",
        "order_by": "count(1) DESC"
    },
    # GROUP BY fails (subquery), others pass
    {
        "select": "data->>'category', count(1)",
        "where": "data->>'status' = 'active'",
        "group_by": "(SELECT 1)",
        "order_by": "count(1) DESC"
    },
    # ORDER BY fails (subquery), others pass
    {
        "select": "data->>'name', count(1)",
        "where": "data->>'role' = 'admin'",
        "group_by": "data->>'name'",
        "order_by": "(SELECT MAX(id) FROM record)"
    },
    # SELECT and WHERE fail
    {
        "select": "count(*)",
        "where": "1=1",
        "group_by": "data->>'category'",
        "order_by": "data->>'name'"
    },
    # SELECT and GROUP BY fail
    {
        "select": "(SELECT version())",
        "where": "data->>'status' = 'active'",
        "group_by": "pg_sleep(1)",
        "order_by": "data->>'name'"
    },
    # WHERE and ORDER BY fail
    {
        "select": "data->>'name', count(1)",
        "where": "COALESCE(TRUE, FALSE)",
        "group_by": "data->>'name'",
        "order_by": "random()"
    },
    # GROUP BY and ORDER BY fail
    {
        "select": "data->>'category', count(1)",
        "where": "data->>'status' = 'active'",
        "group_by": "(SELECT 1)",
        "order_by": "version()"
    },
    # Three parts fail
    {
        "select": "count(*)",
        "where": "1=1",
        "group_by": "pg_sleep(1)",
        "order_by": "data->>'name'"
    },
    {
        "select": "(SELECT MAX(id) FROM record)",
        "where": "data->>'a' = data->>'a'",
        "group_by": "data->>'category'",
        "order_by": "random()"
    },
    {
        "select": "data->>'name', count(1)",
        "where": "1=1",
        "group_by": "data->>'name'",
        "order_by": "(SELECT pg_sleep(5))"
    },
    {
        "select": "count(*)",
        "where": "data->>'status' = 'active'",
        "group_by": "(SELECT 1)",
        "order_by": "version()"
    },
    # All four parts fail
    {
        "select": "count(*)",
        "where": "1=1",
        "group_by": "pg_sleep(1)",
        "order_by": "random()"
    },
    {
        "select": "(SELECT version())",
        "where": "data->>'a' = data->>'a'",
        "group_by": "(SELECT pg_sleep(5))",
        "order_by": "(SELECT MAX(id) FROM record)"
    },
    # --- More Postgres-specific attack vectors ---
    "data->>'a' = 'b' TABLESAMPLE SYSTEM (10)",
    "data->>'a' = 'b' TABLESAMPLE BERNOULLI (10)",
    "data->>'a' = 'b' OFFSET 10",
    "data->>'a' = 'b' LIMIT 10 OFFSET 5",
    "data->>'a' = 'b' FETCH FIRST 10 ROWS ONLY",
    "data->>'a' = 'b' FETCH NEXT 10 ROWS ONLY",
    # --- More system function attempts ---
    "pg_backend_pid()",
    "pg_stat_get_backend_pid(1)",
    "pg_stat_get_activity()",
    "pg_stat_get_backend_idset()",
    "pg_read_binary_file('/etc/passwd')",
    "pg_read_file('/etc/passwd', 0, 1000)",
    "pg_ls_dir('/tmp')",
    "pg_stat_file('/etc/passwd')",
    "getpgusername()",
    "current_setting('search_path')",
    "set_config('search_path', 'public', false)",
    # --- More type casting attacks ---
    "(data->>'id')::name",
    "(data->>'id')::cid",
    "(data->>'id')::refcursor",
    "CAST(data->>'id' AS regclass)",
    "CAST(data->>'id' AS oid)",
    # --- More subquery variations ---
    "data->>'a' = ANY(SELECT id FROM record)",
    "data->>'a' = SOME(SELECT id FROM record)",
    "data->>'a' = ALL(SELECT id FROM record)",
    "data->>'a' IN (SELECT id FROM record UNION SELECT id FROM record)",
    # --- More UNION/INTERSECT/EXCEPT attempts ---
    "data->>'a' = 'b' UNION SELECT 1",
    "data->>'a' = 'b' UNION ALL SELECT 1",
    "data->>'a' = 'b' INTERSECT SELECT 1",
    "data->>'a' = 'b' EXCEPT SELECT 1",
    # --- More JOIN attempts ---
    "data->>'a' = 'b' INNER JOIN record ON 1=1",
    "data->>'a' = 'b' NATURAL JOIN record",
    "data->>'a' = 'b' NATURAL INNER JOIN record",
    "data->>'a' = 'b' NATURAL LEFT JOIN record",
    "data->>'a' = 'b' NATURAL RIGHT JOIN record",
    "data->>'a' = 'b' NATURAL FULL JOIN record",
    # --- More CASE with subqueries ---
    {
        "select": "CASE WHEN EXISTS(SELECT 1) THEN data->>'a' ELSE data->>'b' END"
    },
    {
        "select": "CASE WHEN data->>'a' IN (SELECT 'x') THEN data->>'a' ELSE data->>'b' END"
    },
    {
        "order_by": "CASE WHEN (SELECT 1)=1 THEN data->>'name' ELSE data->>'age' END"
    },
    # --- More aggregate function attacks ---
    {"select": "string_agg(*, ',')"},
    {"select": "array_agg(*)"},
    {"select": "json_agg(*)"},
    {"select": "jsonb_agg(*)"},
    {"select": "jsonb_object_agg(*, *)"},
    {"select": "jsonb_object_agg(data->>'key', *)"},
    # --- More window function attempts (should fail) ---
    {"select": "count(1) OVER ()"},
    {"select": "sum((data->>'amount')::numeric) OVER (PARTITION BY data->>'category')"},
    {"select": "row_number() OVER (ORDER BY data->>'name')"},
    {"select": "rank() OVER ()"},
    {"select": "dense_rank() OVER ()"},
    {"select": "percent_rank() OVER ()"},
    {"select": "cume_dist() OVER ()"},
    {"select": "ntile(4) OVER ()"},
    {"select": "lag(data->>'value') OVER ()"},
    {"select": "lead(data->>'value') OVER ()"},
    {"select": "first_value(data->>'value') OVER ()"},
    {"select": "last_value(data->>'value') OVER ()"},
    {"select": "nth_value(data->>'value', 1) OVER ()"},
    # --- More CTE attempts ---
    "WITH t(id) AS (SELECT 1) SELECT data->>'name' FROM t",
    "WITH t AS (SELECT data->>'name') SELECT * FROM t",
    "WITH t AS (SELECT 1) SELECT data->>'name' FROM t WHERE id IN (SELECT id FROM t)",
    # --- More LATERAL attempts ---
    "data->>'a' = 'b' LEFT JOIN LATERAL (SELECT 1) AS t ON TRUE",
    "data->>'a' = 'b' RIGHT JOIN LATERAL (SELECT 1) AS t ON TRUE",
    "data->>'a' = 'b' INNER JOIN LATERAL (SELECT 1) AS t ON TRUE",
    "data->>'a' = 'b' FULL JOIN LATERAL (SELECT 1) AS t ON TRUE",
    # --- More array/JSONB path attempts with subqueries ---
    "ARRAY(SELECT id FROM record) && data->'tags'",
    "ARRAY(SELECT id FROM record) @> data->'tags'",
    "ARRAY(SELECT id FROM record) <@ data->'tags'",
    "jsonb_path_exists(data, '$.items[*] ? (@.id == (SELECT MAX(id) FROM record))')",
    "jsonb_path_query_array(data, '$.items[*] ? (@.value > (SELECT 1))')",
    "jsonb_path_query_first(data, '$.items[*] ? (@.id == (SELECT 1))')",
    # --- More system catalog access ---
    "data->>'a' = (SELECT tablename FROM pg_tables WHERE schemaname = 'public' LIMIT 1)",
    "data->>'a' = (SELECT column_name FROM information_schema.columns WHERE table_name = 'record' LIMIT 1)",
    "data->>'a' = (SELECT proname FROM pg_proc LIMIT 1)",
    "data->>'a' = (SELECT relname FROM pg_class LIMIT 1)",
    "data->>'a' = (SELECT nspname FROM pg_namespace LIMIT 1)",
    # --- More function call attempts ---
    "generate_series(1, 10)",
    "unnest(ARRAY[1,2,3])",
    "unnest(data->'tags')",
    "jsonb_each(data)",
    "jsonb_each_text(data)",
    "jsonb_object_keys(data)",
    "jsonb_populate_record(null::record, data)",
    "jsonb_populate_recordset(null::record, data)",
    "jsonb_to_record(data)",
    "jsonb_to_recordset(data)",
    "jsonb_array_elements(data->'items')",
    "jsonb_array_elements_text(data->'items')",
    # --- More operator attempts ---
    "data->>'a' OPERATOR(pg_catalog.=) 'b'",
    "data->>'a' OPERATOR(pg_catalog.+) 1",
    # --- More expression attempts ---
    "data->>'a' = (SELECT 1) + (SELECT 1)",
    "data->>'a' = (SELECT 1) * (SELECT 1)",
    "data->>'a' = (SELECT 1) / (SELECT 1)",
    "data->>'a' = (SELECT 1) - (SELECT 1)",
    # --- More boolean logic with subqueries ---
    "data->>'a' = 'b' AND EXISTS(SELECT 1)",
    "data->>'a' = 'b' OR EXISTS(SELECT 1)",
    "NOT EXISTS(SELECT 1)",
    "data->>'a' = 'b' AND (SELECT 1) = 1",
    "data->>'a' = 'b' OR (SELECT 1) = 1",
    # --- Constant evaluation tautologies in WHERE ---
    {"where": "data->>'a' = 'b' OR 10/2=5"},
    {"where": "data->>'a' = 'b' OR coalesce(null, 1)=1"},
    {"where": "data->>'a' = 'b' OR nullif(1,1) is null"},
    {"where": "data->>'a' = 'b' OR abs(-5)=5"},
    {"where": "data->>'a' = 'b' OR (1+2)*3=9"},
    {"where": "coalesce(null, 5+5)=10"},
    {"where": "abs(-10)/2=5"},
    {"where": "nullif(10/2, 5) is null"},
    # --- Constant evaluation tautologies in multi-clause queries ---
    {
        "select": "data->>'name'",
        "where": "10/2=5",
        "group_by": "data->>'name'"
    },
    {
        "select": "data->>'name'",
        "where": "coalesce(null, 1)=1",
        "order_by": "data->>'name'"
    },
    {
        "select": "data->>'category', count(1)",
        "where": "nullif(1,1) is null",
        "group_by": "data->>'category'"
    },
    {
        "select": "data->>'name'",
        "where": "abs(-100)=100 AND data->>'status'='active'",
        "order_by": "data->>'name'"
    },
    {
        "select": "data->>'name'",
        "where": "data->>'status'='active' OR 5+5=10"
    },
    # --- Nested constant evaluation ---
    {"where": "coalesce(null, coalesce(null, 1))=1"},
    {"where": "abs(abs(-5))=5"},
    {"where": "nullif(coalesce(null, 1), 1) is null"},
    {"where": "coalesce(nullif(1,1), 5)=5"},
    # --- Constants in CASE conditions (all must fail) ---
    {
        "select": "CASE WHEN 10/2=5 THEN data->>'a' ELSE data->>'b' END"
    },
    {
        "select": "CASE WHEN coalesce(null, 1)=1 THEN data->>'a' ELSE data->>'b' END"
    },
    {
        "select": "CASE WHEN abs(-1)=1 THEN data->>'a' ELSE data->>'b' END"
    },
    {
        "order_by": "CASE WHEN 5+5=10 THEN data->>'name' ELSE data->>'age' END"
    },
    {
        "group_by": "CASE WHEN nullif(1,1) is null THEN data->>'a' ELSE data->>'b' END"
    },
]
