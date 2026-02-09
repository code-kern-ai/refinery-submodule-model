# injection_tests.py
import os
import sys
import importlib

# Whitelist: map test module names to literal module paths for safe import_module use
ALLOWED_TEST_MODULE_PATHS = {
    "complex_queries": "tests.complex_queries",
    "complexity_cases": "tests.complexity_cases",
    "injection_cases": "tests.injection_cases",
    "multi_clause_cases": "tests.multi_clause_cases",
    "order_group_cases": "tests.order_group_cases",
    "subquery_cases": "tests.subquery_cases",
    "tautology_cases": "tests.tautology_cases",
    "valid_cases": "tests.valid_cases",
    "window_function_cases": "tests.window_function_cases",
}


def _safe_import_test_module(module_name: str):
    """Import a test module only if it is in the whitelist; uses literal paths for import_module."""
    if module_name not in ALLOWED_TEST_MODULE_PATHS:
        raise ValueError(f"Invalid test module (not in whitelist): {module_name}")
    # Literal-only dispatch so import_module never receives a dynamic value
    if module_name == "complex_queries":
        return importlib.import_module("tests.complex_queries")
    if module_name == "complexity_cases":
        return importlib.import_module("tests.complexity_cases")
    if module_name == "injection_cases":
        return importlib.import_module("tests.injection_cases")
    if module_name == "multi_clause_cases":
        return importlib.import_module("tests.multi_clause_cases")
    if module_name == "order_group_cases":
        return importlib.import_module("tests.order_group_cases")
    if module_name == "subquery_cases":
        return importlib.import_module("tests.subquery_cases")
    if module_name == "tautology_cases":
        return importlib.import_module("tests.tautology_cases")
    if module_name == "valid_cases":
        return importlib.import_module("tests.valid_cases")
    if module_name == "window_function_cases":
        return importlib.import_module("tests.window_function_cases")
    raise ValueError(f"Invalid test module (not in whitelist): {module_name}")


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

def run_tests():
    # Dynamic import from tests/ folder; only whitelisted module names to prevent code injection
    test_files = [
        f[:-3]
        for f in os.listdir("tests")
        if f.endswith(".py")
        and f != "__init__.py"
        and f[:-3] in ALLOWED_TEST_MODULE_PATHS
    ]

    all_valid = []
    all_invalid = []

    for test_file in test_files:
        module = _safe_import_test_module(test_file)
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
