# injection_tests.py
import os
import sys
import importlib

def _get_validate():
    try:
        from .sql_validator import validate_sql_clause
        return validate_sql_clause
    except Exception:
        this_dir = os.path.dirname(__file__)
        parent_of_pkg = os.path.abspath(os.path.join(this_dir, ".."))
        if parent_of_pkg not in sys.path:
            sys.path.insert(0, parent_of_pkg)
        mod = importlib.import_module("sql_validator")
        return mod.validate_sql_clause

validate_sql_clause = _get_validate()

# Whitelist of test module names (from tests/ folder). Used to avoid dynamic import.
_ALLOWED_TEST_MODULES = {
    "complex_queries",
    "complexity_cases",
    "injection_cases",
    "multi_clause_cases",
    "order_group_cases",
    "subquery_cases",
    "tautology_cases",
    "valid_cases",
    "window_function_cases",
}


def _import_test_module(test_file: str):
    """Import a test module by name using literal paths only (no dynamic import argument)."""
    if test_file == "complex_queries":
        return importlib.import_module("tests.complex_queries")
    if test_file == "complexity_cases":
        return importlib.import_module("tests.complexity_cases")
    if test_file == "injection_cases":
        return importlib.import_module("tests.injection_cases")
    if test_file == "multi_clause_cases":
        return importlib.import_module("tests.multi_clause_cases")
    if test_file == "order_group_cases":
        return importlib.import_module("tests.order_group_cases")
    if test_file == "subquery_cases":
        return importlib.import_module("tests.subquery_cases")
    if test_file == "tautology_cases":
        return importlib.import_module("tests.tautology_cases")
    if test_file == "valid_cases":
        return importlib.import_module("tests.valid_cases")
    if test_file == "window_function_cases":
        return importlib.import_module("tests.window_function_cases")
    return None


def run_tests():
    # Import from tests/ folder via whitelist; only allowed module names are loaded.
    test_files = [
        f[:-3]
        for f in os.listdir("tests")
        if f.endswith(".py") and f != "__init__.py" and f[:-3] in _ALLOWED_TEST_MODULES
    ]

    all_valid = []
    all_invalid = []

    for test_file in test_files:
        module = _import_test_module(test_file)
        if module is None:
            continue
        if hasattr(module, "VALID_CASES"):
            all_valid.extend(module.VALID_CASES)
        if hasattr(module, "INVALID_CASES"):
            all_invalid.extend(module.INVALID_CASES)

    print(f"=== Running {len(all_valid)} VALID CASES ===")
    pass_count = 0
    for sql in all_valid:
        if isinstance(sql, dict):
            extend_allowed_nodes = {"select", "where", "group", "order", "ordered"}
            rejection_reason = validate_sql_clause(extend_allowed_nodes=extend_allowed_nodes, **sql)
            if any(rejection_reason.values() if isinstance(rejection_reason, dict) else [rejection_reason]):
                print(f"FALSE NEGATIVE: {sql} => {rejection_reason}")
            else:
                pass_count += 1
        else:
            rejection_reason = validate_sql_clause(where=sql)
            if rejection_reason:
                print(f"FALSE NEGATIVE: {sql} => {rejection_reason}")
            else:
                pass_count += 1
    print(f"Passed: {pass_count}/{len(all_valid)}")

    print(f"\n=== Running {len(all_invalid)} INVALID CASES ===")
    pass_count = 0
    for sql in all_invalid:
        if isinstance(sql, dict):
            extend_allowed_nodes = {"select", "where", "group", "order", "ordered"}
            rejection_reason = validate_sql_clause(extend_allowed_nodes=extend_allowed_nodes, **sql)
            if isinstance(rejection_reason, dict):
                if not any(rejection_reason.values()):
                    print(f"FALSE POSITIVE: {sql}")
                else:
                    pass_count += 1
            elif not rejection_reason:
                print(f"FALSE POSITIVE: {sql}")
            else:
                pass_count += 1
        else:
            rejection_reason = validate_sql_clause(where=sql)
            if not rejection_reason:
                print(f"FALSE POSITIVE: {sql}")
            else:
                pass_count += 1
    print(f"Passed: {pass_count}/{len(all_invalid)}")

if __name__ == "__main__":
    run_tests()
