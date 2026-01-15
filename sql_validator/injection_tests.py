# injection_tests.py
# only meant to be run as a script from the console for testing!
# thats also the reason for the somewhat convoluted import logic :D

import os
import sys
import importlib


def _get_validate():
    # Try normal package-relative import first (works when run as a module).
    try:
        from .sql_validator import validate_sql_clause

        return validate_sql_clause
    except Exception:
        # Fallback: running as a standalone script. Import the module as a package.
        # Determine path to parent of `sql_helper` (i.e. src/util)
        this_dir = os.path.dirname(__file__)  # .../src/util/sql_helper
        parent_of_pkg = os.path.abspath(os.path.join(this_dir, ".."))  # .../src/util

        # Ensure that parent_of_pkg is on sys.path so importlib can find the package `sql_helper`
        if parent_of_pkg not in sys.path:
            sys.path.insert(0, parent_of_pkg)

        # Import using package name `sql_helper`. This will give sibling modules correct package context,
        # so their relative imports (e.g. `from .constants import ...`) will work.
        mod = importlib.import_module("sql_helper.sql_helper_none_submodule")
        return mod.validate_sql_clause


# get the function (works whether run directly or as package)
validate_sql_clause = _get_validate()


if __name__ == "__main__":
    VALID_CASES = [
        # --- Basic tests ---
        "r.data->>'name' = 'Alice'",
        "data->>'name' = 'Alice'",
        "data->>'age'::int > 30",
        "data @> '{\"verified\": true}'",
        "(data->>'score')::float >= 4.5",
        "lower(data->>'email') LIKE '%@example.com'",
        "data->'tags' ? 'premium'",
        "(data->>'status') IN ('active', 'pending')",
        "to_tsvector('english', data->>'bio') @@ plainto_tsquery('developer')",
        "(data->'meta'->>'role') = 'admin' AND (data->>'disabled')::boolean = false",
        "jsonb_array_length(data->'projects') > 0",
        "coalesce(data->>'nickname', data->>'name') = 'Anonymous'",
        "data->'preferences' @> '{\"theme\": \"dark\"}'",
        "(data->>'country') IS NOT NULL",
        "(data->'profile'->>'age')::int BETWEEN 18 AND 30",
        "jsonb_typeof(data->'settings') = 'object'",
        "(to_tsvector('english', data->>'summary') @@ phraseto_tsquery('machine learning'))",
        "(data->>'rating')::numeric * 2 > 9",
        "(data->>'joined_at')::date < current_date",
        "(data->>'department') NOT ILIKE 'sales%'",
        "data->>'value' ILIKE '%--%'",
        "data->>'pattern' LIKE '%/*comment*/%'",
        "data->>'text' ~ '^[a-zA-Z0-9_]+$'",  #
        "data->>'text' ~* '^[a-zA-Z0-9_]+$'",  # case-insensitive regex match
        "data->>'text' LIKE '%;%'",
        # --- detailed tests ---
        # --- Type casting coverage ---
        "(data->>'id')::int = 42",
        "(data->>'price')::numeric < 99.99",
        "(data->>'score')::float >= 4.5",
        "(data->>'created_at')::timestamp > now() - interval '7 days'",
        "(data->>'birthday')::date = current_date - interval '30 years'",
        "(data->>'is_active')::boolean = true",
        "(data->>'comment')::text ILIKE '%hello%'",
        # --- String functions ---
        "upper(data->>'title') LIKE 'HELLO%'",
        "trim(data->>'name') = 'Alice'",
        "substring(data->>'description', 1, 5) = 'Hello'",
        "concat(data->>'first_name', ' ', data->>'last_name') = 'Alice Smith'",
        "coalesce(data->>'nickname', data->>'username', 'guest') = 'guest'",
        "length(data->>'bio') > 20",
        # --- JSONB functions ---
        "jsonb_array_length(data->'items') > 1",
        "jsonb_typeof(data->'profile') = 'object'",
        "jsonb_exists(data, 'email')",
        "jsonb_exists_any(data, ARRAY['phone','email'])",
        "jsonb_exists_all(data, ARRAY['first_name','last_name'])",
        # --- Date/time functions ---
        "(data->>'updated_at')::timestamp > now() - interval '1 day'",
        "date_trunc('day', (data->>'joined_at')::timestamp) = current_date::timestamp",
        "age(now(), (data->>'birthdate')::timestamp) > interval '18 years'",
        "to_timestamp(data->>'epoch', 'YYYY-MM-DD HH24:MI:SS') < now()",
        # --- Full text search ---
        "to_tsvector('english', data->>'content') @@ plainto_tsquery('search term')",
        "to_tsvector('simple', data->>'tags') @@ phraseto_tsquery('fast car')",
        "to_tsvector('english', coalesce(data->>'summary','')) @@ plainto_tsquery('ai')",
        # --- Regex & pattern matching ---
        "data->>'username' ~ '^[a-zA-Z0-9_]{3,16}$'",
        "data->>'email' ~* '^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$'",
        "(data->>'url') ~* '^https?://'",
        "data->>'comment' !~ 'spam'",
        "data->>'comment' !~* 'bannedword'",
        # --- Logical combinations ---
        "(data->>'verified')::boolean = true AND (data->>'role') = 'admin'",
        "((data->>'status') = 'active' OR (data->>'status') = 'pending') AND (data->>'deleted')::boolean = false",
        "(data->>'priority')::int BETWEEN 1 AND 5",
        "(data->>'rating')::numeric * 2 > 9",
        # --- Mixed casts and functions ---
        "lower(coalesce(data->>'category', '')) LIKE '%tech%'",
        "(to_tsvector('english', data->>'notes') @@ plainto_tsquery('performance')) AND jsonb_array_length(data->'attachments') > 0",
        "(data->>'last_login')::timestamp > (now() - interval '30 days')",
        "upper(data->>'country') = 'US'",
        "(data->>'joined_at')::timestamp < now() AND jsonb_typeof(data->'settings') = 'object'",
        "data->>'timezone' IN ('UTC', 'CET', 'PST')",
    ]

    INVALID_CASES = [
        # --- Basic SQL injection attempts ---
        "DROP TABLE users",
        "DELETE FROM users WHERE id = 1",
        "UPDATE users SET role='admin'",
        "INSERT INTO users VALUES ('x')",
        "data->>'name' = 'Alice'; DROP TABLE users",
        "data->>'name' = 'Bob' -- comment injection",
        "data->>'role' = (SELECT pg_sleep(5))",
        "(SELECT COUNT(*) FROM users) > 0",
        "pg_terminate_backend(pid)",
        "-- just a comment",
        "/* block comment */",
        "id = ''",  # doesn't use allowed column prefix
        "data->>'age' > 30; DELETE FROM users",  # injection attempt
        "data->>'name' = 'Alice' /* comment */",  # comment injection
        "data->>'status' = 'active' OR 1=1",  # tautology injection
        "1=1",
        "(data->>'is_active')::boolean = true OR TRUE",
        "((r.data->>'metadata')::JSON->>'modified_by') = ((r.data->>'metadata')::JSON->>'created_by'); DORP"
        # Simple tautologies / always-true
        "'1'='1'",
        "0=0",
        "''=''",
        "'a'='a' OR 'b'='b'",
        "username = 'admin' OR 1=1",
        "username = 'admin' OR 'x'='x'",
        "id = 1 OR 1=1",
        "TRUE",
        "1::BOOLEAN",
        # Classic string-termination + boolean
        "' OR '1'='1'",
        "' OR '1'='1' --",
        '" OR "1"="1',
        "' OR 'x'='x' /*",
        "') OR ('1'='1",
        "') OR 1=1 --",
        # Comment injection (inline / block)
        "1=1 --",
        "1=1; --",
        "1=1 /*",
        "username = 'bob' -- comment",
        "' OR 1=1; /*",
        # UNION-based data exfiltration (non‑destructive examples)
        "' UNION SELECT 'a','b','c'",
        "' UNION SELECT NULL,NULL,NULL --",
        "1 UNION SELECT 1,2,3",
        "' UNION SELECT username,password FROM users --",
        # Stacked / multiple statements (DBs that allow it)
        "1=1; DROP TABLE users",
        "1=1; UPDATE users SET role='admin' WHERE id=1",
        "1=1; DELETE FROM users",
        "'; INSERT INTO users(name) VALUES('x'); --",
        # Time-based / sleep injection (blind)
        "pg_sleep(5)=0",
        "1=1; SELECT pg_sleep(5); --",
        "IF(1=1, SLEEP(5), 0)=0",
        "WAITFOR DELAY '00:00:05' --",
        "SELECT CASE WHEN (1=1) THEN pg_sleep(5) ELSE pg_sleep(0) END --",
        # Error-based / cast-induced
        "1/0 = 0",
        "CAST((SELECT count(*) FROM users) AS text) = 'x'",
        "' || (SELECT version()) || ' = 'x",
        "convert(int, (select top 1 name from users)) = 0",
        # Subqueries (boolean / existence)
        "EXISTS(SELECT 1 FROM users WHERE is_admin = true)",
        "(SELECT COUNT(*) FROM users) > 0",
        "id IN (SELECT id FROM users WHERE role='admin')",
        "id = (SELECT id FROM users LIMIT 1)"
        # Function / backend abuse (Postgres / MySQL)
        "pg_terminate_backend(pid) IS NOT NULL",
        "pg_sleep(10) IS NULL",
        "LOAD_FILE('/etc/passwd') IS NOT NULL",
        "@@version_comment LIKE '%'",
        # JSON / JSONB / arrow operators (Postgres style)
        "data->>'name' = 'Alice'; DROP TABLE users"
        "data->>'name' = 'Bob' --"
        "data->>'role' = (SELECT pg_sleep(5))"
        "data->>'name' = 'admin' OR '1'='1'"
        # Quoting / escaping attempts and encodings
        "%27 OR %271%27=%271",
        "\\' OR \\'1\\'=\\'1\\'",
        "0x61646d696e = 0x61646d696e",
        "char(97)+char(100)+char(109)='adm'",
        # Logical manipulations with parentheses
        "('a'='a') OR ('b'='c')",
        "(1=1) AND (SELECT COUNT(*) FROM users) > 0",
        # Blind boolean tricks
        "SUBSTRING((SELECT version()),1,1) = 'P'",
        "ASCII(SUBSTRING((SELECT user()),1,1)) > 64",
        "(SELECT CASE WHEN (LENGTH((SELECT password FROM users LIMIT 1))>0) THEN 1 ELSE 0 END)=1",
        "((SELECT 1 FROM users LIMIT 1) IS NOT NULL)",
        # Keyword and operator insertion
        "OR TRUE",
        "AND FALSE",
        "IS NULL OR 1=1",
        "LIKE '%' OR 1=1",
        "IN (1,2,3) OR 1=1",
        # Short, evasive payloads and obfuscation
        "OR/**/1=1",
        "OR%09%09%091=1",
        "OR 1=1",
        "OR 1=1; -- -",
        "1=1/*comment*/",
        # Semi-benign test payloads (simulate an attack but non-destructive)
        "' OR '1'='1' -- TEST",
        "' UNION SELECT 'TEST1','TEST2' --",
        "1=1 /*INJECTION_TEST*/",
        "username = 'x' OR 'inj_test'='inj_test'",
        "record.data->>'name' = 'Alice'",  # doesn't use allowed column prefix
        # more tests"id = 1",
        "id = ?",
        "username = 'admin'",
        "active = TRUE",
        "deleted IS NULL",
        "created_at >= '2024-01-01'",
        "created_at BETWEEN '2024-01-01' AND '2024-12-31'",
        "price > 0",
        "price >= 100 AND price <= 500",
        "quantity <> 0",
        "status IN ('open','closed','pending')",
        "status NOT IN ('deleted','archived')",
        "email LIKE '%@example.com'",
        "name LIKE 'A%'",
        "description LIKE '%error_%' ESCAPE '\\'",
        "tags @> ARRAY['security']",
        "json_data ->> 'role' = 'admin'",
        "json_data -> 'meta' ->> 'flag' = '1'",
        "EXISTS (SELECT 1 FROM orders o WHERE o.user_id = users.id AND o.total > 100)",
        "NOT EXISTS (SELECT 1 FROM bans b WHERE b.user_id = users.id)",
        "(age < 18 OR age >= 65)",
        "(first_name = 'John' OR (last_name = 'Doe' AND city = 'Berlin'))",
        "LENGTH(password) < 8",
        "CHAR_LENGTH(comment) > 1024",
        "REGEXP_LIKE(phone, '^[0-9]{10}$')",
        "email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
        "score IS NOT DISTINCT FROM 0.0",
        "COALESCE(country, 'unknown') = 'DE'",
        "MOD(id, 2) = 0",
        "(x / NULLIF(y,0)) > 10",
        "created_at::date = CURRENT_DATE",
        "event_time >= now() - INTERVAL '7 days'",
        "inet_client_addr() = '192.0.2.1'",
        "ST_Intersects(geom, ST_MakeEnvelope(0,0,10,10))",
        "ARRAY_LENGTH(path,1) > 3",
        "score::text = '100'",
        "id IN (SELECT user_id FROM admins)",
        "EXISTS (SELECT 1 FROM payments p WHERE p.user_id = users.id AND p.amount = (SELECT MAX(amount) FROM payments))",
        "uuid_col = '00000000-0000-0000-0000-000000000000'",
        "concat(first_name, ' ', last_name) = 'Alice Smith'",
        "LOWER(username) = LOWER('Admin')",
        "UPPER(language) = 'EN'",
        "SUBSTRING(code FROM 1 FOR 3) = 'ERR'",
        "POSITION('needle' IN haystack) > 0",
        "TRIM(both ' ' FROM status) = 'ok'",
        "created_at >= '2024-01-01' AND created_at < '2025-01-01'",
        "id BETWEEN ? AND ?",
        "MATCH(title, body) AGAINST('+security -public' IN BOOLEAN MODE)",
        "(a = b) IS TRUE",
        "(a = b) IS FALSE",
        "(a = b) IS UNKNOWN",
        "priority = LEAST(5, COALESCE(priority,5))",
        "score >= ALL (SELECT score FROM benchmarks WHERE category = outer_table.category)",
        "price = ANY (ARRAY[9.99,19.99,29.99])",
        "jsonb_path_exists(payload, '$.payment[?(@.status==\"failed\")]')",
        "row_num = 1",
        "(col1 IS NULL AND col2 IS NOT NULL) OR (col3 > 1000)",
        "(CASE WHEN flag = 1 THEN 1 ELSE 0 END) = 1",
        "id = CAST(? AS INTEGER)",
    ]

    print("=== VALID CASES ===")
    for sql in VALID_CASES:
        rejection_reason = validate_sql_clause(sql)
        if rejection_reason:
            print(
                "FALSE NEGATIVES (rejected but shouldn't):", sql, "=>", rejection_reason
            )
        # else:
        #     print("Correctly accepted:", sql)

    print("\n=== INVALID CASES ===")
    for sql in INVALID_CASES:
        rejection_reason = validate_sql_clause(sql)
        if not rejection_reason:
            print("FALSE POSITIVES (not rejected but should):", sql)
        # else:
        #     print("Correctly rejected:", sql, "=>", rejection_reason)
