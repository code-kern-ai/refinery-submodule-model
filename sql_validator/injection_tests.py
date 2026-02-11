# injection_tests.py
import os
import re
import sys
import importlib

# Only allow alphanumeric and underscore in test module names (no path traversal or injection).
_SAFE_TEST_MODULE_PATTERN = re.compile(r"^[a-zA-Z0-9_]+$")


def safe_import_test_module(test_file: str):
    """
    Import a test module by name after validating against the whitelist pattern.
    test_file must match _SAFE_TEST_MODULE_PATTERN (alphanumeric and underscore only).
    Exported so parent repos can reuse for safe dynamic test discovery.
    """
    if not _SAFE_TEST_MODULE_PATTERN.match(test_file):
        raise ValueError(f"Invalid test module name (whitelist): {test_file!r}")
    # test_file is restricted by whitelist above — cannot be __proto__/path traversal
    return importlib.import_module("tests." + test_file)  # nosemgrep: python.lang.security.audit.non-literal-import.non-literal-import


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
    # Dynamic import from tests/ folder; only allow whitelisted module names.
    test_files = [f[:-3] for f in os.listdir("tests") if f.endswith(".py") and f != "__init__.py"]

    all_valid = []
    all_invalid = []

    for test_file in test_files:
        if not _SAFE_TEST_MODULE_PATTERN.match(test_file):
            continue
        module = safe_import_test_module(test_file)
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
