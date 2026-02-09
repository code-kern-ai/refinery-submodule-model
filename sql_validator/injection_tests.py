# injection_tests.py
import os
import re
import sys
import importlib

# Whitelist: only allow Python identifier-style module names (no path traversal or arbitrary code load)
_ALLOWED_TEST_MODULE_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _safe_import_test_module(name: str):
    """Import a test module by name; name must match whitelist (valid Python identifier)."""
    if not _ALLOWED_TEST_MODULE_PATTERN.match(name):
        raise ValueError("Disallowed test module name: {!r}".format(name))
    # name is constrained by _ALLOWED_TEST_MODULE_PATTERN above — only safe identifiers, no arbitrary code load
    return importlib.import_module("tests.{}".format(name))  # nosemgrep: python.lang.security.audit.non-literal-import.non-literal-import


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
    # Dynamic import from tests/ folder; only whitelisted module names to prevent arbitrary code load
    test_files = [
        f[:-3]
        for f in os.listdir("tests")
        if f.endswith(".py") and f != "__init__.py" and _ALLOWED_TEST_MODULE_PATTERN.match(f[:-3])
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
