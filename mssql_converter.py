"""
Oracle to MSSQL DDL Converter
Converts Oracle DDL objects to MSSQL-compatible DDL.
"""

import re
from typing import List, Dict, Optional
from dataclasses import dataclass
from oracle_extractor import (
    TableDef, ViewDef, SequenceDef, ProcedureDef, TriggerDef,
    ColumnDef, ConstraintDef, IndexDef
)
from config import ConversionConfig


# Oracle to MSSQL data type mapping
DEFAULT_TYPE_MAPPINGS = {
    'VARCHAR2': 'NVARCHAR',
    'NVARCHAR2': 'NVARCHAR',
    'CHAR': 'CHAR',
    'NCHAR': 'NCHAR',
    'NUMBER': 'DECIMAL',
    'NUMERIC': 'DECIMAL',
    'INTEGER': 'INT',
    'INT': 'INT',
    'SMALLINT': 'SMALLINT',
    'BIGINT': 'BIGINT',
    'FLOAT': 'FLOAT',
    'REAL': 'REAL',
    'DOUBLE PRECISION': 'FLOAT',
    'BINARY_FLOAT': 'FLOAT',
    'BINARY_DOUBLE': 'FLOAT',
    'DATE': 'DATETIME2',
    'TIMESTAMP': 'DATETIME2',
    'TIMESTAMP WITH TIME ZONE': 'DATETIMEOFFSET',
    'TIMESTAMP WITH LOCAL TIME ZONE': 'DATETIME2',
    'CLOB': 'NVARCHAR(MAX)',
    'NCLOB': 'NVARCHAR(MAX)',
    'BLOB': 'VARBINARY(MAX)',
    'BFILE': 'VARBINARY(MAX)',
    'LONG': 'NVARCHAR(MAX)',
    'RAW': 'VARBINARY',
    'LONG RAW': 'VARBINARY(MAX)',
    'ROWID': 'VARCHAR(18)',
    'UROWID': 'VARCHAR(18)',
    'XMLTYPE': 'XML',
    'SDO_GEOMETRY': 'GEOMETRY',
}


# Column name patterns that indicate a score/rating field requiring decimal precision
_SCORE_KEYWORDS = ('SCR', 'SCORE')


def _is_score_column(column_name: str) -> bool:
    """Check if a column name contains score-related keywords requiring decimal(,2) precision."""
    upper_name = column_name.upper().strip()
    return any(keyword in upper_name for keyword in _SCORE_KEYWORDS)


# ---------------------------------------------------------------------------
# Shared Oracle function -> MSSQL conversion utilities
# Used by ViewConverter, ProcedureConverter, and TriggerConverter
# ---------------------------------------------------------------------------

def _convert_decode(sql: str) -> str:
    """Convert Oracle DECODE(expr, search1, result1, ..., default) to CASE WHEN.

    DECODE(a, 1, 'X', 2, 'Y', 'Z')
      -> CASE WHEN a=1 THEN 'X' WHEN a=2 THEN 'Y' ELSE 'Z' END

    DECODE(a, 1, 'X')
      -> CASE WHEN a=1 THEN 'X' ELSE NULL END
    """
    def _replace_decode(match: re.Match) -> str:
        full = match.group(0)
        inner = match.group(1)
        args = _split_function_args(inner)
        if len(args) < 3:
            return full  # Not enough args, leave as-is

        expr = args[0].strip()
        pairs = args[1:]
        case_parts = []
        i = 0
        while i < len(pairs) - 1:
            search_val = pairs[i].strip()
            result_val = pairs[i + 1].strip()
            case_parts.append(f"WHEN {expr}={search_val} THEN {result_val}")
            i += 2
        else_val = "NULL"
        if i < len(pairs):
            else_val = pairs[i].strip()
        case_body = " ".join(case_parts)
        return f"CASE {case_body} ELSE {else_val} END"

    return re.sub(r'(?i)\bDECODE\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)', _replace_decode, sql)


def _split_function_args(text: str) -> List[str]:
    """Split function arguments respecting nested parentheses and quoted strings."""
    args = []
    current = []
    depth = 0
    in_quote = None
    i = 0
    while i < len(text):
        ch = text[i]
        if in_quote:
            current.append(ch)
            if ch == in_quote and i + 1 < len(text) and text[i + 1] == in_quote:
                current.append(text[i + 1])
                i += 1
            elif ch == in_quote:
                in_quote = None
        elif ch in ("'", '"'):
            in_quote = ch
            current.append(ch)
        elif ch == '(':
            depth += 1
            current.append(ch)
        elif ch == ')':
            depth -= 1
            current.append(ch)
        elif ch == ',' and depth == 0:
            args.append(''.join(current).strip())
            current = []
        else:
            current.append(ch)
        i += 1
    if current:
        args.append(''.join(current).strip())
    return args


def _convert_date_functions(sql: str) -> str:
    """Convert Oracle date functions to MSSQL equivalents."""
    # ADD_MONTHS(date, n) -> DATEADD(month, n, date)
    sql = re.sub(
        r'(?i)\bADD_MONTHS\s*\(\s*([^,]+?)\s*,\s*([^)]+)\s*\)',
        r'DATEADD(month, \2, \1)',
        sql,
    )

    # MONTHS_BETWEEN(d1, d2) -> DATEDIFF(month, d2, d1)
    # Note: Oracle returns fractional months; MSSQL DATEDIFF returns integer
    sql = re.sub(
        r'(?i)\bMONTHS_BETWEEN\s*\(\s*([^,]+?)\s*,\s*([^)]+)\s*\)',
        r'DATEDIFF(month, \2, \1)',
        sql,
    )

    # LAST_DAY(date) -> EOMONTH(date)
    sql = re.sub(
        r'(?i)\bLAST_DAY\s*\(\s*([^)]+)\s*\)',
        r'EOMONTH(\1)',
        sql,
    )

    # NEXT_DAY(date, 'DAY') -> DATEADD(day, (target_dow - current_dow + 7) % 7, CAST(date AS DATE))
    # Simplified: use a helper approach — just mark for manual review
    def _replace_next_day(m: re.Match) -> str:
        date_arg = m.group(1).strip()
        day_arg = m.group(2).strip()
        return f"/* NEXT_DAY conversion: manually replace with DATEADD logic for {day_arg} after {date_arg} */"

    sql = re.sub(r'(?i)\bNEXT_DAY\s*\(\s*([^,]+?)\s*,\s*([^)]+)\s*\)', _replace_next_day, sql)

    # TRUNC(date) -> CAST(... AS DATE)
    # TRUNC(date, 'YYYY') -> DATEFROM-parts equivalent
    # TRUNC(date, 'MM') -> first of month
    # TRUNC(number, n) -> ROUND with manual consideration
    sql = re.sub(
        r"(?i)\bTRUNC\s*\(\s*([^,]+?)\s*,\s*'YYYY'\s*\)",
        r"DATEFROMPARTS(YEAR(\1), 1, 1)",
        sql,
    )
    sql = re.sub(
        r"(?i)\bTRUNC\s*\(\s*([^,]+?)\s*,\s*'MM'\s*\)",
        r"DATEFROMPARTS(YEAR(\1), MONTH(\1), 1)",
        sql,
    )
    sql = re.sub(
        r"(?i)\bTRUNC\s*\(\s*([^,]+?)\s*,\s*'Q'\s*\)",
        r"DATEFROMPARTS(YEAR(\1), ((MONTH(\1)-1)/3)*3+1, 1)",
        sql,
    )
    sql = re.sub(
        r"(?i)\bTRUNC\s*\(\s*([^,]+?)\s*,\s*'DD'\s*\)",
        r"CAST(\1 AS DATE)",
        sql,
    )
    # TRUNC with single arg (date or number) — default truncates to day
    sql = re.sub(
        r'(?i)\bTRUNC\s*\(\s*([^)]+)\s*\)',
        r'CAST(\1 AS DATE)',
        sql,
    )

    # NEW_TIME(date, tz1, tz2) -> manual conversion needed
    sql = re.sub(
        r'(?i)\bNEW_TIME\s*\([^)]+\)',
        r'/* NEW_TIME: manual timezone conversion required */',
        sql,
    )

    # EXTRACT(YEAR FROM date) is already standard SQL — no change needed
    # EXTRACT(MONTH FROM date) — same

    return sql


def _convert_other_functions(sql: str) -> str:
    """Convert additional Oracle functions not covered elsewhere."""
    # NVL2(expr, val_if_not_null, val_if_null) -> CASE WHEN expr IS NOT NULL THEN val_if_not_null ELSE val_if_null END
    def _replace_nvl2(m: re.Match) -> str:
        args = _split_function_args(m.group(1))
        if len(args) >= 3:
            return f"CASE WHEN {args[0].strip()} IS NOT NULL THEN {args[1].strip()} ELSE {args[2].strip()} END"
        return m.group(0)
    sql = re.sub(r'(?i)\bNVL2\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)', _replace_nvl2, sql)

    # GREATEST(a, b, ...) -> MSSQL 2022+ supports GREATEST, otherwise use CASE
    # For broad compatibility, keep GREATEST (SQL Server 2022+ supports it)
    # But add a comment for older versions
    # LEAST -> same (SQL Server 2022+)
    # We leave these as-is for SQL Server 2022+ compatibility

    # LNNVL(condition) -> condition IS NOT FALSE (or just condition)
    sql = re.sub(r'(?i)\bLNNVL\s*\(\s*([^)]+)\s*\)', r'\1 IS NOT FALSE', sql)

    # SYS_GUID() -> NEWID()
    sql = re.sub(r'(?i)\bSYS_GUID\s*\(\s*\)', 'NEWID()', sql)

    # UID -> SYSTEM_USER (already handled in convert_default_value, add here too)
    sql = re.sub(r'(?i)(?<![.])\bUSER(?![_(])', 'SYSTEM_USER', sql)

    # COALESCE is already standard SQL — no change
    # NULLIF is already standard SQL — no change

    # SIGN, ABS, CEIL, FLOOR, POWER, SQRT, ROUND, MOD, EXP, LOG, LN
    # Most are standard. LN -> LOG in MSSQL
    sql = re.sub(r'(?i)\bLN\s*\(\s*([^)]+)\s*\)', r'LOG(\1)', sql)
    # MOD(a, b) -> a % b
    sql = re.sub(r'(?i)\bMOD\s*\(\s*([^,]+?)\s*,\s*([^)]+)\s*\)', r'(\1 % \2)', sql)

    # BITAND(a, b) -> a & b
    sql = re.sub(r'(?i)\bBITAND\s*\(\s*([^,]+?)\s*,\s*([^)]+)\s*\)', r'(\1 & \2)', sql)

    # SOUNDEX is already standard SQL — no change

    return sql


def _convert_string_functions(sql: str) -> str:
    """Convert Oracle string functions to MSSQL equivalents."""
    # TO_CHAR(expr) -> CONVERT(VARCHAR, expr) or FORMAT for dates
    # Already handled partially; handle multi-arg TO_CHAR here
    # TO_CHAR(date, 'format') -> CONVERT or FORMAT — mark for manual review for complex formats
    def _replace_to_char(m: re.Match) -> str:
        args = _split_function_args(m.group(1))
        if len(args) >= 2:
            expr = args[0].strip()
            fmt = args[1].strip()
            return f"/* TO_CHAR with format: manually convert format {fmt} for {expr} */"
        return f"CONVERT(VARCHAR, {args[0].strip()})"
    sql = re.sub(r'(?i)\bTO_CHAR\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)', _replace_to_char, sql)

    # TO_DATE(date_str, 'format') -> CONVERT or CAST — mark complex formats
    def _replace_to_date(m: re.Match) -> str:
        args = _split_function_args(m.group(1))
        if len(args) >= 2:
            return f"/* TO_DATE with format: manually convert format {args[1].strip()} for {args[0].strip()} */"
        return f"CONVERT(DATETIME2, {args[0].strip()})"
    sql = re.sub(r'(?i)\bTO_DATE\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)', _replace_to_date, sql)

    # TO_NUMBER(str) -> CAST(str AS DECIMAL)
    def _replace_to_number(m: re.Match) -> str:
        args = _split_function_args(m.group(1))
        if len(args) >= 2:
            return f"/* TO_NUMBER with format: manually convert format {args[1].strip()} for {args[0].strip()} */"
        return f"CAST({args[0].strip()} AS DECIMAL)"
    sql = re.sub(r'(?i)\bTO_NUMBER\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)', _replace_to_number, sql)

    # LENGTH -> LEN
    sql = re.sub(r'(?i)\bLENGTH\s*\(\s*([^)]+)\s*\)', r'LEN(\1)', sql)

    # SUBSTR(str, start, length) -> SUBSTRING(str, start, length)
    # Handle variable-length expressions (not just \w+)
    sql = re.sub(
        r'(?i)\bSUBSTR\s*\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*([^)]+)\s*\)',
        r'SUBSTRING(\1, \2, \3)',
        sql,
    )

    # INSTR — already partially handled; add 4-arg version
    # INSTR(str, substr, start, occurrence) -> complex, mark for review
    # INSTR(str, substr, start) -> CHARINDEX with custom logic
    def _replace_instr(m: re.Match) -> str:
        args = _split_function_args(m.group(1))
        if len(args) >= 4:
            return f"/* INSTR with occurrence: manually convert INSTR({', '.join(args)}) */"
        elif len(args) == 3:
            return f"CHARINDEX({args[1].strip()}, {args[0].strip()}, {args[2].strip()})"
        elif len(args) == 2:
            return f"CHARINDEX({args[1].strip()}, {args[0].strip()})"
        return m.group(0)
    sql = re.sub(r'(?i)\bINSTR\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)', _replace_instr, sql)

    # REPLACE is already standard SQL — no change
    # UPPER, LOWER are standard — no change
    # INITCAP -> no direct MSSQL equivalent, use custom function
    sql = re.sub(r'(?i)\bINITCAP\s*\(\s*([^)]+)\s*\)',
                 r'/* INITCAP: no direct MSSQL equivalent, implement as scalar function */', sql)

    # CONCAT(a, b) -> a + b (or CONCAT which MSSQL also supports)
    # Keep CONCAT as-is since MSSQL 2012+ supports it

    # TRANSLATE(str, from_str, to_str) -> manual or nested REPLACE
    sql = re.sub(
        r'(?i)\bTRANSLATE\s*\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*([^)]+)\s*\)',
        r'/* TRANSLATE: replace with nested REPLACE or TRANSLATE (SQL Server 2017+) */',
        sql,
    )

    return sql


def _convert_aggregate_functions(sql: str) -> str:
    """Convert Oracle aggregate functions to MSSQL equivalents."""
    # WM_CONCAT -> STRING_AGG(col, ',')
    sql = re.sub(
        r'(?i)\bWM_CONCAT\s*\(\s*([^)]+)\s*\)',
        r"STRING_AGG(\1, ',')",
        sql,
    )

    # LISTAGG — already handled in _convert_view_body; add here for procedures
    sql = re.sub(
        r'(?i)LISTAGG\s*\(\s*([^,]+?)\s*,\s*([^)]+)\s*\)\s*WITHIN\s*GROUP\s*\(\s*ORDER\s*BY\s+([^)]+)\s*\)',
        r'STRING_AGG(\1, \2) WITHIN GROUP (ORDER BY \3)',
        sql,
    )
    # LISTAGG without WITHIN GROUP (rare)
    sql = re.sub(
        r'(?i)LISTAGG\s*\(\s*([^,]+?)\s*,\s*([^)]+)\s*\)',
        r"STRING_AGG(\1, \2)",
        sql,
    )

    # STDDEV -> STDEV
    sql = re.sub(r'(?i)\bSTDDEV\s*\(\s*([^)]+)\s*\)', r'STDEV(\1)', sql)

    # VARIANCE -> VAR
    sql = re.sub(r'(?i)\bVARIANCE\s*\(\s*([^)]+)\s*\)', r'VAR(\1)', sql)

    return sql


def _convert_regex_functions(sql: str) -> str:
    """Convert Oracle REGEXP_* functions. MSSQL has limited support.

    SQL Server 2017+ has STRING_SPLIT and TRANSLATE but no native REGEXP.
    SQL Server 2022+ has GENERATE_SERIES and enhanced string functions but still
    no native regex. Mark for manual review or suggest CLR/like alternatives.
    """
    # REGEXP_LIKE(expr, pattern) -> LIKE or manual review
    def _replace_regexp_like(m: re.Match) -> str:
        args = _split_function_args(m.group(1))
        if len(args) >= 2:
            expr = args[0].strip()
            pattern = args[1].strip()
            return f"/* REGEXP_LIKE({expr}, {pattern}): manually convert to LIKE or use CLR */"
        return m.group(0)
    sql = re.sub(r'(?i)\bREGEXP_LIKE\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)', _replace_regexp_like, sql)

    # REGEXP_SUBSTR(expr, pattern, ...) -> manual review
    def _replace_regexp_substr(m: re.Match) -> str:
        args = _split_function_args(m.group(1))
        return f"/* REGEXP_SUBSTR: manually convert — args: {', '.join(args)} */"
    sql = re.sub(r'(?i)\bREGEXP_SUBSTR\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)', _replace_regexp_substr, sql)

    # REGEXP_REPLACE(expr, pattern, replacement, ...) -> manual review
    def _replace_regexp_replace(m: re.Match) -> str:
        args = _split_function_args(m.group(1))
        return f"/* REGEXP_REPLACE: manually convert — args: {', '.join(args)} */"
    sql = re.sub(r'(?i)\bREGEXP_REPLACE\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)', _replace_regexp_replace, sql)

    # REGEXP_INSTR — same treatment
    def _replace_regexp_instr(m: re.Match) -> str:
        args = _split_function_args(m.group(1))
        return f"/* REGEXP_INSTR: manually convert — args: {', '.join(args)} */"
    sql = re.sub(r'(?i)\bREGEXP_INSTR\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)', _replace_regexp_instr, sql)

    # REGEXP_COUNT — same treatment
    def _replace_regexp_count(m: re.Match) -> str:
        args = _split_function_args(m.group(1))
        return f"/* REGEXP_COUNT: manually convert — args: {', '.join(args)} */"
    sql = re.sub(r'(?i)\bREGEXP_COUNT\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)', _replace_regexp_count, sql)

    return sql


def _convert_dbms_functions(sql: str) -> str:
    """Convert or mark Oracle DBMS_* package calls for manual review."""
    # DBMS_OUTPUT.PUT_LINE -> PRINT
    sql = re.sub(r'(?i)\bDBMS_OUTPUT\.PUT_LINE\s*\(\s*([^)]+)\s*\)', r'PRINT \1', sql)

    # DBMS_RANDOM.VALUE([low, high]) -> RAND() or custom
    sql = re.sub(
        r'(?i)\bDBMS_RANDOM\.VALUE\s*\(\s*([^,]+?)\s*,\s*([^)]+)\s*\)',
        r'/* DBMS_RANDOM.VALUE: use RAND() * (\2 - \1) + \1 */',
        sql,
    )
    sql = re.sub(r'(?i)\bDBMS_RANDOM\.VALUE\s*\(\s*\)', r'RAND()', sql)

    # DBMS_UTILITY.GET_TIME -> manual
    sql = re.sub(
        r'(?i)\bDBMS_UTILITY\.GET_TIME\s*\(\s*\)',
        r'/* DBMS_UTILITY.GET_TIME: use DATEDIFF(millisecond, start_time, GETDATE()) */',
        sql,
    )

    # Generic DBMS_* — mark for review
    sql = re.sub(
        r'(?i)\bDBMS_\w+\.\w+\s*\(',
        r'/* MANUAL REVIEW: Oracle DBMS package call — find MSSQL equivalent */',
        sql,
    )

    return sql


def _convert_all_oracle_functions(sql: str) -> str:
    """Apply all Oracle function conversions in the correct order.

    This is the single entry point used by all converters.
    """
    sql = _convert_decode(sql)
    sql = _convert_date_functions(sql)
    sql = _convert_string_functions(sql)
    sql = _convert_other_functions(sql)
    sql = _convert_aggregate_functions(sql)
    sql = _convert_regex_functions(sql)
    sql = _convert_dbms_functions(sql)
    return sql


def convert_data_type(
    oracle_type: str,
    data_length: int = 0,
    data_precision: int = 0,
    data_scale: int = 0,
    char_length: int = 0,
    config: ConversionConfig = None,
    column_name: str = ""
) -> str:
    """Convert Oracle data type to MSSQL data type."""
    mappings = DEFAULT_TYPE_MAPPINGS.copy()
    if config and config.type_mappings:
        mappings.update(config.type_mappings)

    oracle_type_upper = oracle_type.upper().strip()

    # Get base type without parameters
    base_type = oracle_type_upper.split('(')[0].strip()

    mssql_type = mappings.get(base_type, oracle_type_upper)

    # Handle type-specific conversions
    if mssql_type in ('NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR', 'VARBINARY'):
        if oracle_type_upper == 'CLOB' or oracle_type_upper == 'NCLOB':
            return 'NVARCHAR(MAX)'
        elif oracle_type_upper == 'BLOB':
            return 'VARBINARY(MAX)'
        elif char_length > 0:
            return f"{mssql_type}({char_length})"
        elif data_length > 0:
            return f"{mssql_type}({data_length})"
        else:
            return f"{mssql_type}(255)"

    elif mssql_type in ('DECIMAL', 'NUMERIC'):
        # Score columns (SCR, SCORE) always need 2 decimal places
        if _is_score_column(column_name):
            if data_precision > 0:
                return f"{mssql_type}({data_precision},2)"
            return 'DECIMAL(10,2)'

        if data_precision > 0 and data_scale > 0:
            return f"{mssql_type}({data_precision},{data_scale})"
        elif data_precision > 0:
            return f"{mssql_type}({data_precision},0)"
        elif data_length > 0:
            # Oracle NUMBER without precision/scale often stores large integers
            if data_length <= 4:
                return 'INT'
            elif data_length <= 8:
                return 'BIGINT'
            else:
                return 'DECIMAL(38,10)'
        else:
            return 'DECIMAL(18,2)'  # Default for NUMBER

    elif mssql_type == 'DATETIME2':
        return 'DATETIME2(6)'

    elif mssql_type == 'DATETIMEOFFSET':
        return 'DATETIMEOFFSET(6)'

    elif mssql_type == 'INT' and base_type == 'NUMBER':
        # Score columns should not become INT
        if _is_score_column(column_name):
            return 'DECIMAL(10,2)'
        if data_scale == 0 and data_precision <= 10:
            return 'INT'
        elif data_scale == 0 and data_precision <= 19:
            return 'BIGINT'

    return mssql_type


def remove_schema_prefix(name: str, config: ConversionConfig) -> str:
    """Remove Oracle schema prefix from object name."""
    if config.remove_schema_prefix and '.' in name:
        return name.split('.')[-1]
    return name


def quote_identifier(name: str) -> str:
    """Quote MSSQL identifier with square brackets."""
    # Remove any existing schema prefix
    clean_name = name.split('.')[-1]
    return f"[{clean_name}]"


def convert_default_value(
    default_value: str,
    column_type: str,
    config: ConversionConfig
) -> Optional[str]:
    """Convert Oracle default value to MSSQL-compatible format."""
    if not default_value:
        return None
    
    default = default_value.strip()
    
    # Remove Oracle-specific quotes
    if default.startswith("'") and default.endswith("'"):
        default = default[1:-1]
    
    # Convert SYSDATE to GETDATE()
    if config.convert_sysdate_to_getdate:
        default = re.sub(r'(?i)SYSDATE', 'GETDATE()', default)
        default = re.sub(r'(?i)SYSTIMESTAMP', 'SYSDATETIME()', default)
        default = re.sub(r'(?i)CURRENT_TIMESTAMP', 'CURRENT_TIMESTAMP', default)
    
    # Convert USER to SUSER_SNAME() or SYSTEM_USER
    default = re.sub(r'(?i)(?<![.])USER(?![_(])', 'SYSTEM_USER', default)
    
    # Convert UID to a default value
    default = re.sub(r'(?i)\bUID\b', '0', default)
    
    # Remove TO_DATE wrapper - extract the date literal
    to_date_match = re.search(r"TO_DATE\s*\(\s*'([^']+)'", default, re.IGNORECASE)
    if to_date_match:
        date_str = to_date_match.group(1)
        default = f"'{date_str}'"
    
    # Convert NVL to ISNULL
    default = re.sub(r'(?i)\bNVL\s*\(', 'ISNULL(', default)
    
    # Convert sequence.NEXTVAL handling (done separately for identity)
    
    # Wrap string defaults in quotes if needed
    if column_type and any(t in column_type.upper() for t in ('NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR')):
        if not default.startswith("'") and not default.startswith('(') and not re.match(r'^[A-Z_]+\(', default, re.IGNORECASE):
            default = f"'{default}'"
    
    return default


def convert_sequence_to_identity(seq_name: str, sequences: List[SequenceDef]) -> str:
    """Generate IDENTITY column definition from Oracle sequence."""
    for seq in sequences:
        if seq.name.upper() == seq_name.upper() or \
           seq_name.upper().endswith(seq.name.upper()) or \
           seq.name.upper().endswith(seq_name.upper().replace('_ID', '').replace('SEQ', '')):
            return f"IDENTITY({seq.last_number}, {seq.increment_by})"
    return "IDENTITY(1, 1)"


class TableConverter:
    """Converts Oracle table definitions to MSSQL CREATE TABLE statements."""

    def __init__(self, config: ConversionConfig, sequences: List[SequenceDef] = None):
        self.config = config
        self.sequences = sequences or []

    def convert(self, table: TableDef) -> str:
        """Convert a table definition to MSSQL DDL."""
        table_name = remove_schema_prefix(table.name, self.config)
        lines = []
        
        lines.append(f"IF OBJECT_ID('{self.config.target_schema}.{table_name}', 'U') IS NOT NULL")
        lines.append(f"    DROP TABLE {self.config.target_schema}.{quote_identifier(table_name)};")
        lines.append("GO")
        lines.append("")
        
        if table.comments:
            lines.append(f"-- {table.comments}")
        
        lines.append(f"CREATE TABLE {self.config.target_schema}.{quote_identifier(table_name)} (")
        
        column_lines = []
        primary_key_cols = []
        unique_constraints = []
        check_constraints = []
        foreign_keys = []

        # Process constraints
        for constraint in table.constraints:
            if constraint.type == 'P':
                primary_key_cols = constraint.columns
            elif constraint.type == 'U':
                unique_constraints.append(constraint)
            elif constraint.type == 'C' and constraint.search_condition:
                check_constraints.append(constraint)
            elif constraint.type == 'R':
                foreign_keys.append(constraint)

        identity_column = None
        if self.config.handle_auto_increment and len(primary_key_cols) == 1:
            identity_column = primary_key_cols[0]

        # Process columns
        for col in table.columns:
            col_lines = self._convert_column(col, table.constraints, identity_column)
            column_lines.append(f"    {col_lines}")
        
        # Build the statement
        all_parts = column_lines[:]
        
        # Add primary key
        if primary_key_cols:
            pk_cols = ', '.join([quote_identifier(c) for c in primary_key_cols])
            pk_name = f"PK_{table_name}"
            all_parts.append(f"    CONSTRAINT {pk_name} PRIMARY KEY ({pk_cols})")
        
        # Add unique constraints
        for uc in unique_constraints:
            uc_cols = ', '.join([quote_identifier(c) for c in uc.columns])
            all_parts.append(f"    CONSTRAINT UQ_{table_name}_{uc.name} UNIQUE ({uc_cols})")
        
        # Add check constraints
        for cc in check_constraints:
            condition = self._convert_check_condition(cc.search_condition)
            all_parts.append(f"    CONSTRAINT CK_{table_name}_{cc.name} CHECK ({condition})")
        
        # Join all parts
        lines.append(',\n'.join(all_parts))
        lines.append(");")
        lines.append("GO")
        lines.append("")
        
        # Add foreign keys separately
        for fk in foreign_keys:
            if fk.referenced_table:
                fk_lines = self._convert_foreign_key(table_name, fk)
                lines.append(fk_lines)
        
        # Add indexes
        if self.config.include_indexes:
            for idx in table.indexes:
                if not self._is_constraint_index(idx, table.constraints):
                    idx_lines = self._convert_index(idx)
                    lines.append(idx_lines)
        
        # Add column comments as extended properties
        for col in table.columns:
            col_comment = self._get_column_comment(table, col)
            if col_comment:
                lines.append(self._generate_extended_property(table_name, col.name, col_comment))
        
        # Add table comment
        if table.comments:
            lines.append(self._generate_extended_property(table_name, None, table.comments))
        
        return '\n'.join(lines)

    def _convert_column(
        self,
        col: ColumnDef,
        constraints: List[ConstraintDef],
        identity_column: Optional[str]
    ) -> str:
        """Convert a single column definition."""
        parts = [quote_identifier(col.name)]
        
        # Determine if this is part of a primary key with sequence
        is_pk = False
        for c in constraints:
            if c.type == 'P' and col.name in c.columns:
                is_pk = True
                break
        
        # Data type
        mssql_type = convert_data_type(
            col.data_type, col.data_length, col.data_precision,
            col.data_scale, col.char_length, self.config,
            column_name=col.name
        )
        parts.append(mssql_type)
        
        # Identity for auto-increment primary keys
        if identity_column and col.name == identity_column:
            parts.append("IDENTITY(1,1)")
        
        # Default value
        default = convert_default_value(col.data_default, mssql_type, self.config)
        if default:
            parts.append(f"DEFAULT {default}")
        
        # Nullable
        if not col.nullable:
            parts.append("NOT NULL")
        elif is_pk:
            parts.append("NOT NULL")  # PK columns are always NOT NULL
        
        return ' '.join(parts)

    def _convert_check_condition(self, condition: str) -> str:
        """Convert Oracle check condition to MSSQL-compatible format."""
        if not condition:
            return ""

        # Remove outer parentheses if present
        condition = condition.strip()
        if condition.startswith('(') and condition.endswith(')'):
            condition = condition[1:-1]

        # Common conversions
        condition = re.sub(r'(?i)\bSYSDATE\b', 'GETDATE()', condition)
        condition = re.sub(r'(?i)\bNVL\s*\(', 'ISNULL(', condition)

        # Apply full Oracle function conversions
        condition = _convert_all_oracle_functions(condition)

        return condition

    def _convert_foreign_key(self, table_name: str, fk: ConstraintDef) -> str:
        """Convert foreign key constraint."""
        fk_cols = ', '.join([quote_identifier(c) for c in fk.columns])
        ref_table = remove_schema_prefix(fk.referenced_table, self.config)
        ref_cols = ', '.join([quote_identifier(c) for c in fk.referenced_columns])
        
        sql = f"ALTER TABLE {self.config.target_schema}.{quote_identifier(table_name)}\n"
        sql += f"    ADD CONSTRAINT FK_{table_name}_{fk.name} FOREIGN KEY ({fk_cols})\n"
        sql += f"    REFERENCES {self.config.target_schema}.{quote_identifier(ref_table)} ({ref_cols})"
        
        if fk.delete_rule == 'CASCADE':
            sql += "\n    ON DELETE CASCADE"
        elif fk.delete_rule == 'SET NULL':
            sql += "\n    ON DELETE SET NULL"
        
        sql += ";"
        sql += "\nGO\n"
        return sql

    def _convert_index(self, idx: IndexDef) -> str:
        """Convert index definition."""
        table_name = remove_schema_prefix(idx.table_name, self.config)
        idx_name = remove_schema_prefix(idx.name, self.config)
        
        unique = "UNIQUE " if idx.uniqueness == "UNIQUE" else ""
        cols = ', '.join([quote_identifier(c) for c in idx.columns])
        
        sql = f"CREATE {unique}INDEX {quote_identifier(idx_name)} "
        sql += f"ON {self.config.target_schema}.{quote_identifier(table_name)} ({cols});"
        sql += "\nGO\n"
        return sql

    def _is_constraint_index(self, idx: IndexDef, constraints: List[ConstraintDef]) -> bool:
        """Check if index is backing a constraint (PK/UK)."""
        for c in constraints:
            if c.type in ('P', 'U') and idx.name == c.name:
                return True
        return False

    def _generate_extended_property(self, table_name: str, column_name: Optional[str], comment: str) -> str:
        """Generate sp_addextendedproperty for comments."""
        # Escape single quotes in comment
        comment = comment.replace("'", "''")
        
        if column_name:
            sql = f"EXEC sp_addextendedproperty "
            sql += f"@name = N'MS_Description', @value = N'{comment}', "
            sql += f"@level0type = N'SCHEMA', @level0name = N'{self.config.target_schema}', "
            sql += f"@level1type = N'TABLE', @level1name = N'{table_name}', "
            sql += f"@level2type = N'COLUMN', @level2name = N'{column_name}';"
        else:
            sql = f"EXEC sp_addextendedproperty "
            sql += f"@name = N'MS_Description', @value = N'{comment}', "
            sql += f"@level0type = N'SCHEMA', @level0name = N'{self.config.target_schema}', "
            sql += f"@level1type = N'TABLE', @level1name = N'{table_name}';"
        
        sql += "\nGO\n"
        return sql

    def _get_column_comment(self, table: TableDef, col: ColumnDef) -> Optional[str]:
        """Get column comment from extracted data."""
        return table.column_comments.get(col.name)


class ViewConverter:
    """Converts Oracle view definitions to MSSQL-compatible views."""

    def __init__(self, config: ConversionConfig, sequences: List[SequenceDef] = None):
        self.config = config
        self.sequences = sequences or []

    def convert(self, view: ViewDef) -> str:
        """Convert a view definition to MSSQL DQL."""
        view_name = remove_schema_prefix(view.name, self.config)

        lines = []
        lines.append(f"IF OBJECT_ID('{self.config.target_schema}.{view_name}', 'V') IS NOT NULL")
        lines.append(f"    DROP VIEW {self.config.target_schema}.{quote_identifier(view_name)};")
        lines.append("GO")
        lines.append("")

        if view.comments:
            lines.append(f"-- {view.comments}")

        # Convert view body
        body = self._convert_view_body(view.text)
        
        lines.append(f"CREATE VIEW {self.config.target_schema}.{quote_identifier(view_name)} AS")
        lines.append(body)
        lines.append("GO")
        lines.append("")
        
        return '\n'.join(lines)

    def _convert_view_body(self, text: str) -> str:
        """Convert Oracle SQL to MSSQL-compatible SQL."""
        sql = text

        # Remove Oracle-specific hints
        sql = re.sub(r'/\*+\([^)]+\)\s*\*/', '', sql)

        # Convert ROWNUM to TOP or ROW_NUMBER()
        sql = self._convert_rownum(sql)

        # Convert CONNECT BY to recursive CTE
        sql = self._convert_connect_by(sql)

        # Convert dual table references
        if self.config.convert_dual_table:
            sql = self._convert_dual(sql)

        # Convert SYSDATE / SYSTIMESTAMP (before _convert_all_oracle_functions)
        sql = re.sub(r'(?i)\bSYSDATE\b', 'GETDATE()', sql)
        sql = re.sub(r'(?i)\bSYSTIMESTAMP\b', 'SYSDATETIME()', sql)

        # Convert NVL -> ISNULL (before _convert_all_oracle_functions)
        sql = re.sub(r'(?i)\bNVL\s*\(', 'ISNULL(', sql)

        # Apply all Oracle function conversions
        sql = _convert_all_oracle_functions(sql)

        # Convert || to + (string concatenation)
        sql = sql.replace('||', '+')

        # Convert sequence.NEXTVAL to NEXT VALUE FOR
        sql = re.sub(r'(?i)(\w+)\.NEXTVAL', r'NEXT VALUE FOR \1', sql)

        # Handle sequence.CURRVAL - MSSQL has no direct equivalent
        if re.search(r'(?i)(\w+)\.CURRVAL', sql):
            sql = re.sub(r'(?i)(\w+)\.CURRVAL',
                         r'/* MANUAL: Replace \1.CURRVAL - use SCOPE_IDENTITY() or a variable */', sql)

        # Remove schema prefixes on object references while preserving alias-qualified columns.
        if self.config.remove_schema_prefix:
            sql = self._remove_object_schema_prefixes(sql)

        return sql

    def _remove_object_schema_prefixes(self, sql: str) -> str:
        """Remove owner prefixes from table/view references without touching alias.column patterns."""
        sql = re.sub(
            r'(?i)\b(FROM|JOIN|UPDATE|INTO|DELETE\s+FROM|MERGE\s+INTO)\s+([A-Z_][A-Z0-9_$#]*)\.([A-Z_][A-Z0-9_$#]*)',
            r'\1 \3',
            sql
        )
        sql = re.sub(
            r'(?i)(,\s*)([A-Z_][A-Z0-9_$#]*)\.([A-Z_][A-Z0-9_$#]*)',
            r'\1\3',
            sql
        )
        return sql

    def _convert_rownum(self, sql: str) -> str:
        """Convert ROWNUM usage to MSSQL equivalent."""
        rownum_match = re.search(r'(?i)\bROWNUM\s*(?:=|<=|<)\s*(\d+)', sql)
        if not rownum_match:
            return sql

        limit = int(rownum_match.group(1))
        if re.search(r'(?i)\bROWNUM\s*<\s*\d+', rownum_match.group(0)):
            limit = max(limit - 1, 0)

        rownum_condition = r'ROWNUM\s*(?:=|<=|<)\s*\d+'
        sql = re.sub(rf'(?i)\s+WHERE\s+{rownum_condition}\s+AND\s+', ' WHERE ', sql, count=1)
        sql = re.sub(rf'(?i)\s+AND\s+{rownum_condition}\b', '', sql, count=1)
        sql = re.sub(rf'(?i)\s+WHERE\s+{rownum_condition}\b', '', sql, count=1)
        sql = re.sub(r'(?i)\bSELECT\s+', f'SELECT TOP {limit} ', sql, count=1)
        return sql

    def _convert_dual(self, sql: str) -> str:
        """Remove DUAL table references."""
        sql = re.sub(r'(?i)\s+FROM\s+DUAL\s*$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'(?i)\s+FROM\s+DUAL\s*,', ',', sql)
        sql = re.sub(r'(?i)\s+FROM\s+DUAL\s+WHERE', ' WHERE', sql)
        return sql

    def _convert_connect_by(self, sql: str) -> str:
        """Convert CONNECT BY hierarchical queries to recursive CTE.

        Handles simple patterns:
            SELECT ... FROM table
            [WHERE start_condition]
            CONNECT BY [NOCYCLE] PRIOR child = parent
            [ORDER SIBLINGS BY ...]

        Converts to:
            WITH cte_hierarchy AS (
                SELECT ... FROM table WHERE start_condition   -- anchor
                UNION ALL
                SELECT ... FROM table c JOIN cte_hierarchy p  -- recursive
                    ON c.child = p.parent
            )
            SELECT ... FROM cte_hierarchy
        """
        # Pattern: CONNECT BY [NOCYCLE] PRIOR col1 = col2
        cb_match = re.search(
            r'(?i)(CONNECT\s+BY\s+(?:NOCYCLE\s+)?(?:PRIOR\s+)?(\w+(?:\.\w+)?)\s*=\s*(?:PRIOR\s+)?(\w+(?:\.\w+)?))',
            sql,
        )
        if not cb_match:
            # Try without PRIOR keyword
            cb_match = re.search(
                r'(?i)(CONNECT\s+BY\s+(?:NOCYCLE\s+)?(\w+(?:\.\w+)?)\s*=\s*(\w+(?:\.\w+)?))',
                sql,
            )
        if not cb_match:
            return sql

        full_clause = cb_match.group(0)
        child_col = cb_match.group(2).strip()
        parent_col = cb_match.group(3).strip()

        is_nocycle = bool(re.search(r'(?i)NOCYCLE', full_clause))

        # Extract the SELECT ... FROM table part before CONNECT BY
        before_cb = sql[:cb_match.start()]
        after_cb = sql[cb_match.end():]

        # Remove ORDER SIBLINGS BY (MSSQL ORDER BY is after the final SELECT)
        siblings_order = ''
        siblings_match = re.search(r'(?i)\s*ORDER\s+SIBLINGS\s+BY\s+([^;\n]+)', after_cb)
        if siblings_match:
            siblings_order = siblings_match.group(1).strip()
            after_cb = after_cb[:siblings_match.start()] + after_cb[siblings_match.end():]

        # Extract START WITH condition
        start_match = re.search(r'(?i)\bSTART\s+WITH\s+(.+?)(?=\s+CONNECT\s+BY|$)', before_cb, re.DOTALL)
        start_condition = start_match.group(1).strip() if start_match else None

        # Remove START WITH from before_cb
        if start_match:
            before_cb = before_cb[:start_match.start()] + before_cb[start_match.end():]

        # Remove CONNECT BY clause from before_cb
        before_cb = before_cb.replace(full_clause, '').strip()

        # Now before_cb should be: SELECT ... FROM table [WHERE ...]
        # Extract the SELECT list and FROM table
        select_match = re.search(r'(?i)\bSELECT\s+(DISTINCT\s+)?(.+?)\bFROM\s+(\w+(?:\.\w+)?(?:\s+\w+)?)', before_cb, re.DOTALL)
        if not select_match:
            return sql  # Can't parse, return as-is

        distinct = select_match.group(1) or ''
        select_list = select_match.group(2).strip()
        from_table = select_match.group(3).strip()

        # Add LEVEL pseudo-column support
        if 'LEVEL' in select_list.upper():
            select_list = select_list + ', 1 AS LEVEL'

        # Build the recursive CTE
        anchor_where = start_condition if start_condition else '1=1'
        recursive_join = f"t.{child_col} = h.{parent_col}"

        cte = f"WITH cte_hierarchy AS (\n"
        cte += f"    -- Anchor: root rows\n"
        cte += f"    SELECT {select_list}\n"
        cte += f"    FROM {from_table}\n"
        cte += f"    WHERE {anchor_where}\n"
        cte += f"    UNION ALL\n"
        cte += f"    -- Recursive: child rows\n"
        cte += f"    SELECT t.*\n"
        cte += f"    FROM {from_table} t\n"
        cte += f"    INNER JOIN cte_hierarchy h ON {recursive_join}\n"
        if is_nocycle:
            cte += f"    WHERE NOT EXISTS (SELECT 1 FROM cte_hierarchy h2 WHERE h2.{child_col} = t.{parent_col})\n"
        cte += f")\n"

        # Build final SELECT
        if siblings_order:
            final_select = f"SELECT {distinct} *\nFROM cte_hierarchy\nORDER BY {siblings_order}"
        else:
            remaining = after_cb.strip()
            # Extract ORDER BY, GROUP BY, etc.
            order_match = re.search(r'(?i)\bORDER\s+BY\s+(.+?)(?:;|$)', remaining, re.DOTALL)
            if order_match:
                order_clause = order_match.group(1).strip().rstrip(';')
                final_select = f"SELECT {distinct} *\nFROM cte_hierarchy\nORDER BY {order_clause}"
            else:
                final_select = f"SELECT {distinct} *\nFROM cte_hierarchy"

        result = cte + final_select

        # Add manual review note
        result = f"/* CONNECT BY converted to recursive CTE — review for correctness */\n" + result

        return result


class SequenceConverter:
    """Converts Oracle sequences to MSSQL equivalents."""

    def __init__(self, config: ConversionConfig):
        self.config = config

    def convert(self, seq: SequenceDef) -> str:
        """Convert Oracle sequence to MSSQL equivalent.

        MSSQL SEQUENCE is available from SQL Server 2012+.
        MINVALUE/MAXVALUE, data type, and CYCLE options are mapped.
        """
        seq_name = remove_schema_prefix(seq.name, self.config)

        # Determine appropriate data type based on min/max values
        data_type = self._determine_sequence_type(seq)

        lines = []
        lines.append(f"-- Oracle sequence: {seq.name}")
        lines.append(f"--   Min: {seq.min_value}, Max: {seq.max_value}, Increment: {seq.increment_by}")
        lines.append(f"IF EXISTS (SELECT * FROM sys.sequences WHERE name = '{seq_name}')")
        lines.append(f"    DROP SEQUENCE {self.config.target_schema}.{quote_identifier(seq_name)};")
        lines.append("GO")
        lines.append("")

        # Create MSSQL sequence (SQL Server 2012+)
        lines.append(f"CREATE SEQUENCE {self.config.target_schema}.{quote_identifier(seq_name)}")
        lines.append(f"    AS {data_type}")
        lines.append(f"    START WITH {seq.last_number}")
        lines.append(f"    INCREMENT BY {seq.increment_by}")

        # MINVALUE / MAXVALUE
        if seq.min_value != 1:  # Oracle default min for ascending
            lines.append(f"    MINVALUE {seq.min_value}")
        if seq.max_value != 9999999999999999999999999999:  # Oracle default max
            # Clamp to BIGINT max if exceeded
            max_val = min(seq.max_value, 9223372036854775807)
            lines.append(f"    MAXVALUE {max_val}")

        # CYCLE option
        if not seq.cycle_flag:
            lines.append(f"    NO CYCLE")
        else:
            lines.append(f"    CYCLE")

        # CACHE option
        if seq.cache_size > 0:
            lines.append(f"    CACHE {seq.cache_size}")
        else:
            lines.append(f"    NO CACHE")

        lines.append("GO")
        lines.append("")

        return '\n'.join(lines)

    def _determine_sequence_type(self, seq: SequenceDef) -> str:
        """Determine the smallest appropriate MSSQL integer type."""
        max_val = max(abs(seq.min_value), abs(seq.max_value))

        if max_val <= 2147483647 and seq.min_value >= -2147483648:
            return 'INT'
        elif max_val <= 9223372036854775807:
            return 'BIGINT'
        else:
            # Oracle supports larger numbers than MSSQL's BIGINT
            # Use DECIMAL(38,0) as fallback (requires SQL Server 2012+ SP1+)
            return 'DECIMAL(38,0)'


class ProcedureConverter:
    """Converts Oracle procedures/functions to MSSQL equivalents."""

    def __init__(self, config: ConversionConfig):
        self.config = config

    def convert(self, proc: ProcedureDef) -> str:
        """Convert Oracle procedure/function to MSSQL."""
        proc_name = remove_schema_prefix(proc.name, self.config)
        
        lines = []
        
        if proc.type == 'PROCEDURE':
            drop_type = 'P'
            create_keyword = 'PROCEDURE'
        else:
            drop_type = 'FN'
            create_keyword = 'FUNCTION'
        
        lines.append(f"IF OBJECT_ID('{self.config.target_schema}.{proc_name}', '{drop_type}') IS NOT NULL")
        lines.append(f"    DROP {'PROCEDURE' if proc.type == 'PROCEDURE' else 'FUNCTION'} {self.config.target_schema}.{quote_identifier(proc_name)};")
        lines.append("GO")
        lines.append("")
        
        # Convert source
        source = self._convert_source(proc.source, proc.arguments, proc.type)
        
        if proc.type == 'FUNCTION':
            # MSSQL functions need RETURNS clause
            source = self._add_returns_clause(source, proc.arguments)
        
        lines.append(source)
        lines.append("GO")
        lines.append("")
        
        return '\n'.join(lines)

    def _convert_source(self, source: str, args: List[Dict], proc_type: str) -> str:
        """Convert procedure/function source code."""
        sql = source

        # Remove Oracle-specific directives
        sql = re.sub(r'(?i)CREATE\s+(OR\s+REPLACE\s+)?', 'CREATE ', sql)

        # Convert parameter modes
        sql = re.sub(r'(?i)\bIN\s+OUT\b', 'OUTPUT', sql)
        sql = re.sub(r'(?i)\bOUT\b', 'OUTPUT', sql)
        # Keep IN parameters without keyword (MSSQL default)

        # Convert parameter types
        for oracle_type, mssql_type in DEFAULT_TYPE_MAPPINGS.items():
            sql = re.sub(r'(?i)\b' + re.escape(oracle_type) + r'\b', mssql_type, sql)

        # Replace VARCHAR2 with NVARCHAR
        sql = re.sub(r'(?i)\bVARCHAR2\b', 'NVARCHAR', sql)

        # Convert Oracle string length syntax
        sql = re.sub(r'(?i)\bNVARCHAR\s*\(\s*\d+\s*\bBYTE\b\s*\)', 'NVARCHAR', sql)
        sql = re.sub(r'(?i)\bNVARCHAR\s*\(\s*\d+\s*\bCHAR\b\s*\)', 'NVARCHAR', sql)

        # Convert boolean
        sql = re.sub(r'(?i)\bTRUE\b', '1', sql)
        sql = re.sub(r'(?i)\bFALSE\b', '0', sql)

        # Convert SYSDATE/SYSTIMESTAMP (before _convert_all_oracle_functions)
        sql = re.sub(r'(?i)\bSYSDATE\b', 'GETDATE()', sql)
        sql = re.sub(r'(?i)\bSYSTIMESTAMP\b', 'SYSDATETIME()', sql)

        # Convert NVL -> ISNULL (before _convert_all_oracle_functions)
        sql = re.sub(r'(?i)\bNVL\s*\(', 'ISNULL(', sql)

        # Apply all Oracle function conversions
        sql = _convert_all_oracle_functions(sql)

        # Convert string concatenation
        sql = sql.replace('||', '+')

        # Convert sequence.NEXTVAL to NEXT VALUE FOR (MSSQL 2012+)
        sql = re.sub(r'(?i)(\w+)\.NEXTVAL', r'NEXT VALUE FOR \1', sql)

        # Handle sequence.CURRVAL - MSSQL has no direct equivalent
        if re.search(r'(?i)(\w+)\.CURRVAL', sql):
            sql = re.sub(r'(?i)(\w+)\.CURRVAL',
                         r'/* MANUAL: Replace \1.CURRVAL with SCOPE_IDENTITY() or stored variable */', sql)

        # Convert exception handling to TRY...CATCH skeleton
        # Note: This is a best-effort regex conversion. Complex PL/SQL requires manual review.
        if re.search(r'(?i)EXCEPTION\s+WHEN\s+OTHERS\s+THEN', sql):
            # Replace EXCEPTION block with END TRY BEGIN CATCH
            sql = re.sub(r'(?i)EXCEPTION\s+WHEN\s+OTHERS\s+THEN', 'END TRY\nBEGIN CATCH', sql)
            # Wrap main body in BEGIN TRY (find the first BEGIN after CREATE)
            sql = re.sub(r'(?i)(CREATE\s+(PROCEDURE|FUNCTION)\s+\w+\s+AS\s+)', r'\1BEGIN TRY\n', sql, count=1)
            # Add END CATCH and closing END if missing
            if not re.search(r'(?i)END\s*;\s*$', sql):
                sql = sql.rstrip() + "\nEND CATCH;\nEND"

        # Remove PRAGMA statements
        sql = re.sub(r'(?i)PRAGMA\s+.*?;?', '', sql)

        # Remove semicolons after END for MSSQL (keep final END;)
        sql = re.sub(r'(?i)(?!END\s*;)\bEND\s*;', 'END', sql)

        return sql

    def _add_returns_clause(self, source: str, args: List[Dict]) -> str:
        """Add RETURNS clause for MSSQL functions."""
        # Find function definition line
        func_match = re.search(r'(?i)(CREATE\s+FUNCTION\s+\w+\s*)\(', source)
        if func_match:
            # Check for explicit RETURN type in extracted arguments.
            return_arg = None
            for arg in args:
                if arg.get('name', '').upper() == 'RETURN':
                    return_arg = arg
                    break
            if return_arg is None:
                for arg in args:
                    if arg.get('in_out') == 'OUT':
                        return_arg = arg
                        break
            if return_arg and return_arg.get('data_type'):
                return_type = convert_data_type(return_arg['data_type'])
                source = re.sub(
                    r'(?i)(CREATE\s+FUNCTION\s+\w+\s*\([^)]*\))',
                    rf'\1 RETURNS {return_type}',
                    source
                )
        
        return source


class TriggerConverter:
    """Converts Oracle triggers to MSSQL equivalents."""

    def __init__(self, config: ConversionConfig):
        self.config = config

    def convert(self, trigger: TriggerDef) -> str:
        """Convert Oracle trigger to MSSQL."""
        trigger_name = remove_schema_prefix(trigger.name, self.config)
        table_name = remove_schema_prefix(trigger.table_name, self.config)
        
        lines = []
        lines.append(f"IF OBJECT_ID('{self.config.target_schema}.{trigger_name}', 'TR') IS NOT NULL")
        lines.append(f"    DROP TRIGGER {self.config.target_schema}.{quote_identifier(trigger_name)};")
        lines.append("GO")
        lines.append("")
        
        # Convert trigger source
        source = self._convert_source(trigger.source, trigger)
        
        lines.append(source)
        lines.append("GO")
        lines.append("")
        
        return '\n'.join(lines)

    def _convert_source(self, source: str, trigger: TriggerDef) -> str:
        """Convert trigger source code."""
        sql = source

        # Convert trigger timing
        sql = re.sub(r'(?i)CREATE\s+(OR\s+REPLACE\s+)?TRIGGER', 'CREATE TRIGGER', sql)

        # Convert BEFORE to INSTEAD OF
        sql = re.sub(r'(?i)\bBEFORE\s+(INSERT|UPDATE|DELETE)', r'INSTEAD OF \1', sql)
        sql = re.sub(r'(?i)\bAFTER\s+(INSERT|UPDATE|DELETE)', r'AFTER \1', sql)

        # Convert FOR EACH ROW
        sql = re.sub(r'(?i)\bFOR\s+EACH\s+ROW', '', sql)

        # Convert :NEW and :OLD references
        sql = re.sub(r':NEW\.', 'INSERTED.', sql)
        sql = re.sub(r':OLD\.', 'DELETED.', sql)

        # Convert raising application error
        sql = re.sub(r'(?i)RAISE_APPLICATION_ERROR\s*\(\s*[^,]+,\s*', 'RAISERROR(N', sql)

        # Convert SYSDATE/SYSTIMESTAMP (before _convert_all_oracle_functions)
        sql = re.sub(r'(?i)\bSYSDATE\b', 'GETDATE()', sql)
        sql = re.sub(r'(?i)\bSYSTIMESTAMP\b', 'SYSDATETIME()', sql)

        # Convert NVL -> ISNULL (before _convert_all_oracle_functions)
        sql = re.sub(r'(?i)\bNVL\s*\(', 'ISNULL(', sql)

        # Apply all Oracle function conversions
        sql = _convert_all_oracle_functions(sql)

        # Convert string concatenation
        sql = sql.replace('||', '+')

        # Convert sequence.NEXTVAL to NEXT VALUE FOR (MSSQL 2012+)
        sql = re.sub(r'(?i)(\w+)\.NEXTVAL', r'NEXT VALUE FOR \1', sql)

        # Handle sequence.CURRVAL - MSSQL has no direct equivalent
        if re.search(r'(?i)(\w+)\.CURRVAL', sql):
            sql = re.sub(r'(?i)(\w+)\.CURRVAL',
                         r'/* MANUAL: Replace \1.CURRVAL with SCOPE_IDENTITY() or stored variable */', sql)

        # Convert EXCEPTION block to TRY...CATCH skeleton
        if re.search(r'(?i)EXCEPTION\s+WHEN\s+OTHERS\s+THEN', sql):
            sql = re.sub(r'(?i)EXCEPTION\s+WHEN\s+OTHERS\s+THEN', 'END TRY\nBEGIN CATCH', sql)
            sql = re.sub(r'(?i)(CREATE\s+TRIGGER\s+\w+\s+ON\s+\w+\s+(?:INSTEAD\s+OF|AFTER)\s+\w+\s+AS\s+)',
                         r'\1BEGIN TRY\n', sql, count=1, flags=re.IGNORECASE)
            sql = sql.rstrip() + "\nEND CATCH;\nEND"

        return sql


class DDLConverter:
    """Main converter that orchestrates all conversions."""

    def __init__(self, config: ConversionConfig):
        self.config = config
        self.table_converter = TableConverter(config)
        self.view_converter = ViewConverter(config)
        self.sequence_converter = SequenceConverter(config)
        self.procedure_converter = ProcedureConverter(config)
        self.trigger_converter = TriggerConverter(config)

    def convert_all(self, extracted_data: Dict) -> Dict:
        """Convert all extracted Oracle objects to MSSQL DDL."""
        sequences = extracted_data.get('sequences', [])

        # Re-initialize converters with sequence info
        self.table_converter = TableConverter(self.config, sequences)
        self.view_converter = ViewConverter(self.config, sequences)

        result = {
            'tables': [],
            'views': [],
            'sequences': [],
            'procedures': [],
            'triggers': [],
        }

        if self.config.include_tables:
            for table in extracted_data.get('tables', []):
                result['tables'].append(self.table_converter.convert(table))

        if self.config.include_views:
            for view in extracted_data.get('views', []):
                result['views'].append(self.view_converter.convert(view))
        
        if self.config.include_sequences:
            for seq in sequences:
                result['sequences'].append(self.sequence_converter.convert(seq))
        
        for proc in extracted_data.get('procedures', []):
            proc_type = proc.type.upper()
            if proc_type == 'PROCEDURE' and self.config.include_procedures:
                result['procedures'].append(self.procedure_converter.convert(proc))
            elif proc_type == 'FUNCTION' and self.config.include_functions:
                result['procedures'].append(self.procedure_converter.convert(proc))
        
        if self.config.include_triggers:
            for trigger in extracted_data.get('triggers', []):
                result['triggers'].append(self.trigger_converter.convert(trigger))
        
        return result

    def generate_full_script(self, converted: Dict = None, extracted_data: Dict = None) -> str:
        """Generate a complete MSSQL migration script."""
        if converted is None:
            if extracted_data is None:
                raise ValueError("Either converted or extracted_data must be provided")
            converted = self.convert_all(extracted_data)
        
        sections = []
        
        # Header
        sections.append("-- ===========================================")
        sections.append("-- Oracle to MSSQL DDL Migration Script")
        sections.append(f"-- Generated by Oracle2MSSQL Converter")
        sections.append("-- ===========================================")
        sections.append("")
        sections.append("SET ANSI_NULLS ON;")
        sections.append("GO")
        sections.append("SET QUOTED_IDENTIFIER ON;")
        sections.append("GO")
        sections.append("")
        
        # Sequences first (for IDENTITY references)
        if converted['sequences']:
            sections.append("-- ===========================================")
            sections.append("-- SEQUENCES")
            sections.append("-- ===========================================")
            sections.append("")
            sections.extend(converted['sequences'])
        
        # Tables
        if converted['tables']:
            sections.append("-- ===========================================")
            sections.append("-- TABLES")
            sections.append("-- ===========================================")
            sections.append("")
            sections.extend(converted['tables'])
        
        # Views
        if converted['views']:
            sections.append("-- ===========================================")
            sections.append("-- VIEWS")
            sections.append("-- ===========================================")
            sections.append("")
            sections.extend(converted['views'])
        
        # Procedures and Functions
        if converted['procedures']:
            sections.append("-- ===========================================")
            sections.append("-- PROCEDURES AND FUNCTIONS")
            sections.append("-- ===========================================")
            sections.append("")
            sections.extend(converted['procedures'])
        
        # Triggers
        if converted['triggers']:
            sections.append("-- ===========================================")
            sections.append("-- TRIGGERS")
            sections.append("-- ===========================================")
            sections.append("")
            sections.extend(converted['triggers'])
        
        return '\n'.join(sections)
