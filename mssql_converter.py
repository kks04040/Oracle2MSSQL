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
        identity_col = None
        
        # Process columns
        for col in table.columns:
            col_lines = self._convert_column(col, table.constraints)
            column_lines.append(f"    {col_lines}")
        
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

    def _convert_column(self, col: ColumnDef, constraints: List[ConstraintDef]) -> str:
        """Convert a single column definition."""
        parts = [quote_identifier(col.name)]
        
        # Determine if this is part of a primary key with sequence
        is_pk = False
        pk_sequence = None
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
        if is_pk and self.config.handle_auto_increment:
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
        
        # Convert date functions
        sql = re.sub(r'(?i)\bSYSDATE\b', 'GETDATE()', sql)
        sql = re.sub(r'(?i)\bSYSTIMESTAMP\b', 'SYSDATETIME()', sql)
        
        # Convert string functions
        sql = re.sub(r'(?i)\bNVL\s*\(', 'ISNULL(', sql)
        sql = re.sub(r'(?i)\bDECODE\s*\(', 'CASE', sql)  # Simplified
        sql = re.sub(r'(?i)\bTO_CHAR\s*\(', 'CONVERT(VARCHAR, ', sql)
        sql = re.sub(r'(?i)\bTO_DATE\s*\(', 'CONVERT(DATETIME2, ', sql)
        sql = re.sub(r'(?i)\bTO_NUMBER\s*\(', 'CAST(', sql)
        sql = re.sub(r'(?i)\bTRUNC\s*\(', 'CAST(', sql)  # Simplified for dates
        
        # Convert LISTAGG to STRING_AGG
        sql = re.sub(r'(?i)LISTAGG\s*\((\w+)\s*,\s*([^)]+)\)\s*WITHIN\s*GROUP\s*\(\s*ORDER\s*BY\s+([^)]+)\)',
                     r'STRING_AGG(\1, \2) WITHIN GROUP (ORDER BY \3)', sql)
        
        # Convert || to +
        sql = sql.replace('||', '+')
        
        # Convert SUBSTR to SUBSTRING
        sql = re.sub(r'(?i)\bSUBSTR\s*\(\s*(\w+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)',
                     r'SUBSTRING(\1, \2, \3)', sql)
        
        # Convert INSTR to CHARINDEX
        sql = re.sub(r'(?i)\bINSTR\s*\(\s*(\w+)\s*,\s*([^,]+)\s*\)',
                     r'CHARINDEX(\2, \1)', sql)

        # Convert sequence.NEXTVAL to NEXT VALUE FOR
        sql = re.sub(r'(?i)(\w+)\.NEXTVAL', r'NEXT VALUE FOR \1', sql)

        # Handle sequence.CURRVAL - MSSQL has no direct equivalent
        # Add warning comment for manual review
        if re.search(r'(?i)(\w+)\.CURRVAL', sql):
            sql = re.sub(r'(?i)(\w+)\.CURRVAL',
                         r'/* MANUAL: Replace \1.CURRVAL - use SCOPE_IDENTITY() or a variable */', sql)

        # Remove schema prefixes
        if self.config.remove_schema_prefix:
            sql = re.sub(r'\b[A-Z_]+\.\b', '', sql)

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
        """Convert CONNECT BY hierarchical queries to recursive CTE."""
        # This is a simplified conversion - complex hierarchies may need manual adjustment
        connect_by_match = re.search(r'(?i)CONNECT\s+BY\s+(PRIOR\s+)?(\w+)\s*=\s*(\w+)', sql)
        if connect_by_match:
            # Convert to recursive CTE structure
            return sql  # Return as-is with comment for manual conversion
        return sql


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
        
        # Convert NULL handling
        sql = re.sub(r'(?i)\bNVL\s*\(', 'ISNULL(', sql)
        
        # Convert string concatenation
        sql = sql.replace('||', '+')

        # Convert date functions
        sql = re.sub(r'(?i)\bSYSDATE\b', 'GETDATE()', sql)
        sql = re.sub(r'(?i)\bTO_DATE\s*\(', 'CONVERT(DATETIME2, ', sql)
        sql = re.sub(r'(?i)\bTO_CHAR\s*\(', 'CONVERT(VARCHAR, ', sql)
        sql = re.sub(r'(?i)\bTO_NUMBER\s*\(', 'CAST(', sql)

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
            # Check for RETURN type in arguments
            for arg in args:
                if arg.get('in_out') == 'OUT' or arg.get('name', '').upper() == 'RETURN':
                    return_type = convert_data_type(arg['data_type'])
                    source = re.sub(
                        r'(?i)(CREATE\s+FUNCTION\s+\w+\s*\([^)]*\))',
                        rf'\1 RETURNS {return_type}',
                        source
                    )
                    break
        
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
        
        # Convert EXCEPTION block to TRY...CATCH skeleton
        if re.search(r'(?i)EXCEPTION\s+WHEN\s+OTHERS\s+THEN', sql):
            sql = re.sub(r'(?i)EXCEPTION\s+WHEN\s+OTHERS\s+THEN', 'END TRY\nBEGIN CATCH', sql)
            sql = re.sub(r'(?i)(CREATE\s+TRIGGER\s+\w+\s+ON\s+\w+\s+(?:INSTEAD\s+OF|AFTER)\s+\w+\s+AS\s+)', 
                         r'\1BEGIN TRY\n', sql, count=1, flags=re.IGNORECASE)
            sql = sql.rstrip() + "\nEND CATCH;\nEND"
        
        # Convert string concatenation
        sql = sql.replace('||', '+')

        # Convert functions
        sql = re.sub(r'(?i)\bNVL\s*\(', 'ISNULL(', sql)
        sql = re.sub(r'(?i)\bSYSDATE\b', 'GETDATE()', sql)

        # Convert sequence.NEXTVAL to NEXT VALUE FOR (MSSQL 2012+)
        sql = re.sub(r'(?i)(\w+)\.NEXTVAL', r'NEXT VALUE FOR \1', sql)

        # Handle sequence.CURRVAL - MSSQL has no direct equivalent
        if re.search(r'(?i)(\w+)\.CURRVAL', sql):
            sql = re.sub(r'(?i)(\w+)\.CURRVAL',
                         r'/* MANUAL: Replace \1.CURRVAL with SCOPE_IDENTITY() or stored variable */', sql)

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
