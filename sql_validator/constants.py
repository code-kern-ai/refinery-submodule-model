from sqlglot import TokenType, expressions as exp

# sqlglot AST node keys allowed when validating full data-block SQL fragments
# (multi-clause: SELECT / WHERE / GROUP BY / HAVING / ORDER BY together).
DATA_BLOCK_EXTENDED_AST_NODE_KEYS = frozenset(
    ("select", "where", "group", "having", "order", "ordered")
)

# Aggregate (and related) expression types for ORDER BY / HAVING column-or-aggregate checks.
HAVING_REQUIRES_GROUP_BY_MSG = "HAVING clause requires GROUP BY clause"

AGGREGATE_EXPRESSION_TYPES = (
    exp.Count,
    exp.Sum,
    exp.Avg,
    exp.Min,
    exp.Max,
    exp.Stddev,
    exp.Variance,
    exp.Corr,
    exp.CovarPop,
    exp.LogicalOr,
    exp.LogicalAnd,
    exp.BitwiseAndAgg,
    exp.BitwiseOrAgg,
    exp.ArrayAgg,
    exp.JSONArrayAgg,
    exp.AnyValue,
)

# only allow operations on this JSON column
# tuple for startswith check
ALLOWED_COLUMN_PREFIX = (
    "data",
    "r.data",
)

DISALLOWED_COLUMN_PREFIX = ("pg_", "load_", "inet_client_addr", "sleep")

# Complexity limits to prevent DoS and overly complex queries
MAX_WINDOW_FUNCTIONS = 5  # Maximum number of window functions per query
MAX_QUERY_LENGTH = 10000  # Maximum characters per clause
MAX_EXPRESSION_DEPTH = 50  # Maximum nesting depth for expressions
MAX_REGEX_LENGTH = 200  # Maximum length of regex patterns
# Regex patterns that indicate potential ReDoS (catastrophic backtracking)
DANGEROUS_REGEX_PATTERNS = (
    r"(\+\+|\*\*|\?\?)",  # Nested quantifiers like a]++, a**, a??
    r"\(\?[^)]*\+",  # Possessive quantifiers in groups
    r"(\([^)]*\))\1*\+",  # Repeated groups with +
)

ALLOWED_NODES = {
    # ============================================================
    # BASIC EXPRESSION NODES
    # ============================================================
    "alias",
    "and",
    "add",
    "any",
    "between",
    "boolean",
    "bytestring",
    "case",
    "cast",
    "coalesce",
    "column",
    "datatype",
    "datatypeparam",
    "distinct",
    "dpipe",  # String concatenation operator ||
    "eq",
    "gt",
    "gte",
    "group",
    "having",
    "if",
    "identifier",
    "ilike",
    "in",
    "interval",
    "is",
    "lambda",
    "like",
    "literal",
    "lt",
    "lte",
    "matchagainst",
    "mul",
    "neg",  # Unary negation
    "not",
    "neq",
    "null",
    "nullif",
    "or",
    "objectidentifier",
    "order",
    "ordered",
    "paren",
    "select",
    "sub",
    "tuple",  # For DISTINCT ON (col1, col2), row comparisons
    "var",
    "where",
    # ============================================================
    # STRING FUNCTION NODES
    # ============================================================
    "anonymous",  # For functions not specifically mapped
    "ascii",
    "chr",
    "concat",
    "decode",
    "encode",
    "format",
    "initcap",
    "left",
    "length",
    "lower",
    "lpad",
    "ltrim",
    "md5",
    "overlay",
    "position",
    "repeat",
    "replace",
    "reverse",
    "right",
    "rpad",
    "rtrim",
    "splitpart",
    "strposition",
    "substring",
    "stringtoarray",
    "translate",
    "trim",
    "upper",
    # Regex nodes
    "regexpilike",
    "regexplike",
    "regexpreplace",
    "regexpmatch",
    # Padding
    "pad",
    "lpad",
    "rpad",
    # ============================================================
    # NUMERIC/MATH FUNCTION NODES
    # ============================================================
    "abs",
    "ceil",
    "div",
    "exp",
    "floor",
    "greatest",
    "least",
    "ln",
    "log",
    "mod",
    "power",
    "pow",
    "rand",
    "round",
    "sign",
    "sqrt",
    "trunc",
    # Trigonometric
    "sin",
    "cos",
    "tan",
    "cot",
    "asin",
    "acos",
    "atan",
    "atan2",
    "sinh",
    "cosh",
    "tanh",
    "pi",
    # ============================================================
    # DATE/TIME FUNCTION NODES
    # ============================================================
    "currentdate",
    "currenttime",
    "currenttimestamp",
    "dateadd",
    "datediff",
    "datepart",
    "datetrunc",
    "extract",
    "justifydays",
    "justifyhours",
    "makedate",
    "makeinterval",
    "maketime",
    "maketimestamp",
    "strtodate",
    "strtotime",
    "timetostr",
    "timestamptrunc",
    "timestampfromparts",
    "tochar",
    "todate",
    "tonumber",
    "totimestamp",
    "unixtotime",
    # ============================================================
    # ARRAY NODES
    # ============================================================
    "array",
    "arrayagg",
    "arrayappend",
    "arraycat",
    "arraycontainsall",
    "arraylength",
    "arrayoverlaps",
    "arrayposition",
    "arrayremove",
    "arrayreplace",
    "arraysize",
    "arraytostring",
    "unnest",
    "explode",  # sqlglot maps unnest to explode
    # ============================================================
    # JSON/JSONB NODES
    # ============================================================
    "jsonarray",
    "jsonarrayagg",
    "jsonbcontains",
    "jsonbcontainsalltopkeys",
    "jsonbexists",
    "jsonbextract",
    "jsonbextractscalar",
    "jsonextract",
    "jsonextractscalar",
    "jsonformat",
    "jsonobject",
    "jsonpath",
    "jsonpathkey",
    "jsonpathroot",
    "tojson",
    "tojsonb",
    # ============================================================
    # AGGREGATE NODES
    # ============================================================
    "anyvalue",
    "avg",
    "bitand",
    "bitor",
    "bitxor",
    "booland",
    "boolor",
    "bitwiseandagg",
    "bitwiseoragg",
    "corr",
    "count",
    "covarpop",
    "groupconcat",
    "logicaland",
    "logicalor",
    "max",
    "min",
    "regrintercept",
    "regrslope",
    "stddev",
    "stddevvariance",
    "stringagg",
    "sum",
    "variance",
    # ============================================================
    # WINDOW FUNCTION NODES
    # ============================================================
    "cumedist",
    "denserank",
    "firstvalue",
    "lag",
    "lastvalue",
    "lead",
    "nthvalue",
    "ntile",
    "percentrank",
    "rank",
    "rownumber",
    "window",
    "windowspec",
    # ============================================================
    # TEXT SEARCH NODES
    # ============================================================
    "tsormatch",
    "tsmatch",
    # ============================================================
    # ENCODING/UUID NODES
    # ============================================================
    "uuid",
}

ALLOWED_TOKEN_TYPES = {
    # Identifiers & Literals
    TokenType.IDENTIFIER,
    TokenType.STRING,
    TokenType.NUMBER,
    TokenType.VAR,
    TokenType.PARAMETER,
    TokenType.PLACEHOLDER,
    TokenType.ALIAS,
    # Data Types
    TokenType.INT,
    TokenType.FLOAT,
    TokenType.DECIMAL,
    TokenType.BIGINT,
    TokenType.SMALLINT,
    TokenType.TINYINT,
    TokenType.DOUBLE,
    TokenType.MONEY,
    TokenType.CHAR,
    TokenType.VARCHAR,
    TokenType.NCHAR,
    TokenType.NVARCHAR,
    TokenType.TEXT,
    TokenType.UUID,
    TokenType.XML,
    TokenType.JSON,
    TokenType.JSONB,
    TokenType.BOOLEAN,
    TokenType.VARBINARY,  # For bytea type
    # Date/Time Types
    TokenType.DATE,
    TokenType.TIMESTAMP,
    TokenType.TIMESTAMPTZ,
    TokenType.TIMETZ,
    TokenType.TIME,
    TokenType.INTERVAL,
    TokenType.CURRENT_DATE,
    TokenType.CURRENT_TIMESTAMP,
    TokenType.CURRENT_TIME,
    # Operators
    TokenType.INET,
    TokenType.NUMRANGE,
    TokenType.UNNEST,
    TokenType.AMP,
    TokenType.HASH_ARROW,
    TokenType.DHASH_ARROW,
    TokenType.DAMP,
    TokenType.ANY,
    TokenType.OPERATOR,
    TokenType.EQ,
    TokenType.NEQ,
    TokenType.LT,
    TokenType.LTE,
    TokenType.GT,
    TokenType.GTE,
    TokenType.PLUS,
    TokenType.DASH,
    TokenType.STAR,
    TokenType.SLASH,
    TokenType.PERCENT,
    TokenType.CARET,
    TokenType.TILDA,
    TokenType.DARROW,
    TokenType.ARROW,
    TokenType.DCOLON,
    TokenType.DOT,
    TokenType.BIT,
    TokenType.DIV,
    TokenType.MOD,
    TokenType.FORMAT,
    # Logical/Comparison
    TokenType.AND,
    TokenType.OR,
    TokenType.NOT,
    TokenType.IN,
    TokenType.BETWEEN,
    TokenType.IS,
    TokenType.NULL,
    TokenType.TRUE,
    TokenType.FALSE,
    TokenType.LIKE,
    TokenType.ILIKE,
    TokenType.IRLIKE,
    TokenType.CASE,
    TokenType.WHEN,
    TokenType.THEN,
    TokenType.ELSE,
    TokenType.END,
    # Brackets & Punctuation
    TokenType.L_PAREN,
    TokenType.R_PAREN,
    TokenType.L_BRACKET,
    TokenType.R_BRACKET,
    TokenType.COMMA,
    TokenType.ESCAPE,
    TokenType.PIPE,
    TokenType.DPIPE,
    TokenType.FROM,
    TokenType.JOIN,
    TokenType.ON,
    TokenType.ORDER_BY,
    TokenType.GROUP_BY,
    TokenType.HAVING,
    TokenType.LIMIT,
    TokenType.OFFSET,
    TokenType.FETCH,
    TokenType.FIRST,
    TokenType.NEXT,
    TokenType.ONLY,
    TokenType.UNION,
    TokenType.INTERSECT,
    TokenType.EXCEPT,
    TokenType.ALL,
    TokenType.DISTINCT,
    # Arrays
    TokenType.ARRAY,
    # Extended/Other
    TokenType.CURRENT_DATE,
    TokenType.CURRENT_TIMESTAMP,
    TokenType.CURRENT_TIME,
    TokenType.ASC,
    TokenType.DESC,
    TokenType.LEFT,
    TokenType.RIGHT,
    TokenType.REPLACE,
    TokenType.SELECT,
    # Window function tokens
    TokenType.OVER,
    TokenType.PARTITION_BY,
    TokenType.ROWS,
    TokenType.ROW,
    TokenType.RANGE,
}

ALLOWED_FUNCS = {
    # ============================================================
    # STRING FUNCTIONS - Safe data manipulation
    # ============================================================
    "ascii",  # Returns ASCII code of first character
    "btrim",  # Trim characters from both sides
    "chr",  # Returns character from ASCII code
    "concat",  # Concatenate strings
    "concat_ws",  # Concatenate with separator
    "format",  # Format string (like sprintf)
    "initcap",  # Capitalize first letter of each word
    "left",  # Left substring
    "length",  # String length
    "lower",  # Lowercase
    "lpad",  # Left pad string
    "ltrim",  # Left trim
    "md5",  # MD5 hash (returns hex string)
    "overlay",  # Replace substring
    "position",  # Find substring position
    "repeat",  # Repeat string N times
    "replace",  # Replace occurrences
    "reverse",  # Reverse string
    "right",  # Right substring
    "rpad",  # Right pad string
    "rtrim",  # Right trim
    "split_part",  # Split and get part
    "strpos",  # Find substring position
    "substring",  # Extract substring
    "translate",  # Replace characters
    "trim",  # Trim whitespace/characters
    "upper",  # Uppercase
    # Regex string functions
    "regexp_replace",  # Regex replace
    "regexp_match",  # Regex match (returns array)
    "regexp_matches",  # Regex matches (set returning, but safe)
    "regexp_split_to_array",  # Split by regex to array
    # ============================================================
    # NUMERIC/MATH FUNCTIONS - Safe calculations
    # ============================================================
    "abs",  # Absolute value
    "cbrt",  # Cube root
    "ceil",  # Ceiling
    "ceiling",  # Ceiling (alias)
    "degrees",  # Radians to degrees
    "div",  # Integer division
    "exp",  # Exponential
    "floor",  # Floor
    "gcd",  # Greatest common divisor
    "lcm",  # Least common multiple
    "ln",  # Natural logarithm
    "log",  # Logarithm
    "log10",  # Base-10 logarithm
    "mod",  # Modulo
    "pi",  # Pi constant
    "power",  # Power/exponent
    "radians",  # Degrees to radians
    "random",  # Random number (non-deterministic but safe)
    "round",  # Round to N decimals
    "scale",  # Scale of numeric
    "sign",  # Sign (-1, 0, 1)
    "sqrt",  # Square root
    "trunc",  # Truncate
    "width_bucket",  # Bucket assignment
    # Trigonometric
    "acos",
    "acosd",
    "acosh",
    "asin",
    "asind",
    "asinh",
    "atan",
    "atand",
    "atanh",
    "atan2",
    "atan2d",
    "cos",
    "cosd",
    "cosh",
    "cot",
    "cotd",
    "sin",
    "sind",
    "sinh",
    "tan",
    "tand",
    "tanh",
    # ============================================================
    # DATE/TIME FUNCTIONS - Safe temporal operations
    # ============================================================
    "age",  # Interval between timestamps
    "clock_timestamp",  # Current timestamp (changes during query)
    "current_date",  # Current date
    "current_time",  # Current time
    "current_timestamp",  # Current timestamp
    "date_part",  # Extract date part
    "date_trunc",  # Truncate to precision
    "extract",  # Extract field from timestamp
    "isfinite",  # Check if finite
    "justify_days",  # Adjust interval days
    "justify_hours",  # Adjust interval hours
    "justify_interval",  # Adjust interval
    "localtime",  # Local time
    "localtimestamp",  # Local timestamp
    "make_date",  # Construct date
    "make_interval",  # Construct interval
    "make_time",  # Construct time
    "make_timestamp",  # Construct timestamp
    "make_timestamptz",  # Construct timestamp with timezone
    "now",  # Current timestamp
    "statement_timestamp",  # Statement start timestamp
    "timeofday",  # Current time as text
    "to_char",  # Format to string
    "to_date",  # Parse string to date
    "to_number",  # Parse string to number
    "to_timestamp",  # Parse string to timestamp
    "transaction_timestamp",  # Transaction start timestamp
    # ============================================================
    # AGGREGATE FUNCTIONS - Safe summarization
    # ============================================================
    "avg",
    "bit_and",
    "bit_or",
    "bit_xor",
    "bool_and",
    "bool_or",
    "count",
    "every",  # Alias for bool_and
    "json_agg",
    "jsonb_agg",
    "max",
    "min",
    "string_agg",
    "sum",
    "array_agg",
    # ============================================================
    # JSON/JSONB FUNCTIONS - Safe JSON manipulation
    # ============================================================
    "json_array_length",
    "json_build_array",
    "json_build_object",
    "json_extract_path",
    "json_extract_path_text",
    "json_object",
    "json_object_keys",  # Returns keys (set returning but safe on data column)
    "json_populate_record",
    "json_strip_nulls",
    "json_typeof",
    "jsonb_array_length",
    "jsonb_build_array",
    "jsonb_build_object",
    "jsonb_exists",
    "jsonb_exists_all",
    "jsonb_exists_any",
    "jsonb_extract_path",
    "jsonb_extract_path_text",
    "jsonb_insert",
    "jsonb_object",
    "jsonb_object_keys",  # Returns keys (set returning but safe on data column)
    "jsonb_path_exists",
    "jsonb_path_match",
    "jsonb_path_query",
    "jsonb_path_query_array",
    "jsonb_path_query_first",
    "jsonb_pretty",
    "jsonb_set",
    "jsonb_set_lax",
    "jsonb_strip_nulls",
    "jsonb_typeof",
    "row_to_json",  # Safe - only operates on allowed data
    "to_json",
    "to_jsonb",
    # ============================================================
    # ARRAY FUNCTIONS - Safe array operations
    # ============================================================
    "array_append",
    "array_cat",
    "array_dims",
    "array_fill",
    "array_length",
    "array_lower",
    "array_ndims",
    "array_position",
    "array_positions",
    "array_prepend",
    "array_remove",
    "array_replace",
    "array_to_string",
    "array_upper",
    "cardinality",
    "string_to_array",
    "unnest",  # Set returning but safe on data arrays
    # ============================================================
    # CONDITIONAL/COMPARISON FUNCTIONS
    # ============================================================
    "coalesce",
    "greatest",
    "least",
    "nullif",
    # ============================================================
    # TEXT SEARCH FUNCTIONS - Safe full-text operations
    # ============================================================
    "phraseto_tsquery",
    "plainto_tsquery",
    "to_tsquery",
    "to_tsvector",
    "ts_headline",
    "ts_rank",
    "ts_rank_cd",
    "websearch_to_tsquery",
    # ============================================================
    # ENCODING/CONVERSION FUNCTIONS - Safe data transformation
    # ============================================================
    "convert",  # Character set conversion
    "convert_from",  # Convert bytea to text
    "convert_to",  # Convert text to bytea
    "decode",  # Decode from text representation
    "encode",  # Encode to text representation
    # ============================================================
    # UUID FUNCTIONS - Safe identifier operations
    # ============================================================
    "gen_random_uuid",  # Generate random UUID
    "uuid_generate_v4",  # Generate random UUID (uuid-ossp extension)
    # ============================================================
    # MISC SAFE FUNCTIONS
    # ============================================================
    "generate_subscripts",  # Generate array subscripts
    "quote_ident",  # Quote identifier safely
    "quote_literal",  # Quote literal safely
    "quote_nullable",  # Quote nullable value
}
