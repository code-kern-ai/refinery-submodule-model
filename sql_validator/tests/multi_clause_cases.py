AGG_EXPR = "data->>'name'"

VALID_CASES = [
    {
        "select": f"{AGG_EXPR}, count({AGG_EXPR}), sum((data->>'running_id')::numeric)",
        "group_by": AGG_EXPR,
    },
    {
        "select": f"{AGG_EXPR}, count({AGG_EXPR}), avg((data->>'running_id')::numeric)",
        "group_by": AGG_EXPR,
    },
    {
        "select": f"{AGG_EXPR}, count({AGG_EXPR}), min((data->>'running_id')::numeric)",
        "group_by": AGG_EXPR,
    },
    {
        "select": f"{AGG_EXPR}, count({AGG_EXPR}), max((data->>'running_id')::numeric)",
        "group_by": AGG_EXPR,
    },
    {
        "select": f"{AGG_EXPR}, count({AGG_EXPR}), stddev((data->>'running_id')::numeric)",
        "group_by": AGG_EXPR,
    },
    {
        "select": f"{AGG_EXPR}, count({AGG_EXPR}), variance((data->>'running_id')::numeric)",
        "group_by": AGG_EXPR,
    },
]

# Add more generated multi-clause valid cases
for i in range(20):
    VALID_CASES.append({
        "select": f"data->>'col{i}'",
        "where": f"data->>'val{i}' = 'foo'",
        "order_by": f"data->>'col{i}' ASC",
    })

INVALID_CASES = [
    {
        "select": "(SELECT MAX(id) FROM record)",
    },
]

for i in range(20):
    INVALID_CASES.append({
        "select": "1",
        "where": f"1=1 -- injection {i}",
    })
    INVALID_CASES.append({
        "select": f"1; drop table users_{i}",
    })
    INVALID_CASES.append({
        "where": f"data->>'a' = 'b' or {i}={i}",
    })
