"""
Oracle DDL Extractor
Extracts DDL for tables, views, sequences, procedures, functions, triggers, and indexes.
"""

import oracledb
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from config import OracleConfig


@dataclass
class ColumnDef:
    """Column definition extracted from Oracle."""
    name: str
    data_type: str
    data_length: int = 0
    data_precision: int = 0
    data_scale: int = 0
    nullable: bool = True
    data_default: Optional[str] = None
    char_length: int = 0
    column_id: int = 0


@dataclass
class ConstraintDef:
    """Constraint definition."""
    name: str
    type: str  # P, U, R, C
    columns: List[str] = field(default_factory=list)
    referenced_table: Optional[str] = None
    referenced_columns: List[str] = field(default_factory=list)
    search_condition: Optional[str] = None
    delete_rule: Optional[str] = None


@dataclass
class IndexDef:
    """Index definition."""
    name: str
    table_name: str
    columns: List[str] = field(default_factory=list)
    uniqueness: str = "NONUNIQUE"
    index_type: str = "NORMAL"


@dataclass
class TableDef:
    """Complete table definition."""
    name: str
    columns: List[ColumnDef] = field(default_factory=list)
    constraints: List[ConstraintDef] = field(default_factory=list)
    indexes: List[IndexDef] = field(default_factory=list)
    comments: str = ""
    column_comments: Dict[str, str] = field(default_factory=dict)


@dataclass
class ViewDef:
    """View definition."""
    name: str
    text: str
    comments: str = ""


@dataclass
class SequenceDef:
    """Sequence definition."""
    name: str
    min_value: int = 1
    max_value: int = 9999999999999999999999999999
    increment_by: int = 1
    cycle_flag: bool = False
    order_flag: bool = False
    cache_size: int = 20
    last_number: int = 1


@dataclass
class ProcedureDef:
    """Procedure/Function definition."""
    name: str
    type: str  # PROCEDURE, FUNCTION
    source: str
    arguments: List[Dict] = field(default_factory=list)


@dataclass
class TriggerDef:
    """Trigger definition."""
    name: str
    table_name: str
    triggering_event: str
    trigger_type: str  # BEFORE/AFTER, ROW/STATEMENT
    source: str


class OracleExtractor:
    """Extracts DDL objects from Oracle database."""

    def __init__(self, config: OracleConfig):
        self.config = config
        self.connection = None

    def connect(self):
        """Establish connection to Oracle database."""
        try:
            self.connection = oracledb.connect(
                user=self.config.username,
                password=self.config.password,
                dsn=self.config.dsn
            )
            print(f"Connected to Oracle database at {self.config.dsn}")
        except Exception as e:
            print(f"Error connecting to Oracle: {e}")
            raise

    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            print("Disconnected from Oracle database")

    def _get_schema(self) -> str:
        """Get the schema name to query."""
        return self.config.schema.upper() if self.config.schema else self.connection.username.upper()

    def extract_tables(self) -> List[TableDef]:
        """Extract all table definitions from the schema."""
        schema = self._get_schema()
        tables = []

        cursor = self.connection.cursor()
        
        # Get tables
        cursor.execute("""
            SELECT table_name 
            FROM all_tables 
            WHERE owner = :schema
            AND iot_name IS NULL
            AND secondary = 'N'
            ORDER BY table_name
        """, schema=schema)
        
        table_names = [row[0] for row in cursor.fetchall()]

        for table_name in table_names:
            table_def = TableDef(name=table_name)
            
            # Get columns
            cursor.execute("""
                SELECT column_name, data_type, data_length, data_precision, 
                       data_scale, nullable, data_default, char_length, column_id
                FROM all_tab_columns
                WHERE owner = :schema AND table_name = :table
                ORDER BY column_id
            """, schema=schema, table=table_name)
            
            for row in cursor.fetchall():
                col = ColumnDef(
                    name=row[0],
                    data_type=row[1],
                    data_length=row[2] or 0,
                    data_precision=row[3] or 0,
                    data_scale=row[4] or 0,
                    nullable=row[5] == 'Y',
                    data_default=row[6],
                    char_length=row[7] or 0,
                    column_id=row[8]
                )
                table_def.columns.append(col)

            # Get constraints
            cursor.execute("""
                SELECT constraint_name, constraint_type, search_condition, 
                       delete_rule, r_constraint_name, r_owner
                FROM all_constraints
                WHERE owner = :schema AND table_name = :table
                AND constraint_type IN ('P', 'U', 'R', 'C')
            """, schema=schema, table=table_name)
            
            constraints_info = []
            for row in cursor.fetchall():
                constraints_info.append({
                    'name': row[0],
                    'type': row[1],
                    'search_condition': row[2],
                    'delete_rule': row[3],
                    'r_constraint_name': row[4],
                    'r_owner': row[5]
                })

            # Get constraint columns
            for ci in constraints_info:
                cursor.execute("""
                    SELECT column_name
                    FROM all_cons_columns
                    WHERE owner = :schema AND table_name = :table 
                    AND constraint_name = :constraint
                    ORDER BY position
                """, schema=schema, table=table_name, constraint=ci['name'])
                
                cols = [r[0] for r in cursor.fetchall()]
                
                ref_table = None
                ref_cols = []
                if ci['type'] == 'R' and ci['r_constraint_name']:
                    ref_owner = ci.get('r_owner') or schema
                    cursor.execute("""
                        SELECT r.table_name, cc.column_name
                        FROM all_constraints r
                        JOIN all_cons_columns cc ON r.constraint_name = cc.constraint_name 
                            AND r.owner = cc.owner
                        WHERE r.owner = :ref_owner AND r.constraint_name = :ref_constraint
                        ORDER BY cc.position
                    """, ref_owner=ref_owner, ref_constraint=ci['r_constraint_name'])
                    
                    ref_rows = cursor.fetchall()
                    if ref_rows:
                        ref_table = ref_rows[0][0]
                        ref_cols = [r[1] for r in ref_rows]

                constraint = ConstraintDef(
                    name=ci['name'],
                    type=ci['type'],
                    columns=cols,
                    referenced_table=ref_table,
                    referenced_columns=ref_cols,
                    search_condition=ci['search_condition'],
                    delete_rule=ci['delete_rule']
                )
                table_def.constraints.append(constraint)

            # Get indexes
            cursor.execute("""
                SELECT i.index_name, i.uniqueness, i.index_type
                FROM all_indexes i
                WHERE i.owner = :schema AND i.table_name = :table
                AND i.index_type NOT IN ('LOB')
            """, schema=schema, table=table_name)
            
            for row in cursor.fetchall():
                idx = IndexDef(
                    name=row[0],
                    table_name=table_name,
                    uniqueness=row[1],
                    index_type=row[2]
                )
                
                cursor.execute("""
                    SELECT column_name
                    FROM all_ind_columns
                    WHERE index_owner = :schema AND index_name = :index
                    ORDER BY column_position
                """, schema=schema, index=row[0])
                
                idx.columns = [r[0] for r in cursor.fetchall()]
                table_def.indexes.append(idx)

            # Get table comment
            cursor.execute("""
                SELECT comments FROM all_tab_comments
                WHERE owner = :schema AND table_name = :table
            """, schema=schema, table=table_name)
            
            comment_row = cursor.fetchone()
            if comment_row and comment_row[0]:
                table_def.comments = comment_row[0]

            # Get column comments
            cursor.execute("""
                SELECT column_name, comments FROM all_col_comments
                WHERE owner = :schema AND table_name = :table
                AND comments IS NOT NULL
            """, schema=schema, table=table_name)
            
            for col_row in cursor.fetchall():
                table_def.column_comments[col_row[0]] = col_row[1]

            tables.append(table_def)

        cursor.close()
        return tables

    def extract_views(self) -> List[ViewDef]:
        """Extract all view definitions from the schema."""
        schema = self._get_schema()
        views = []

        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT view_name, text, comments
            FROM all_views v
            LEFT JOIN all_tab_comments c ON v.owner = c.owner AND v.view_name = c.table_name
            WHERE v.owner = :schema
            ORDER BY view_name
        """, schema=schema)

        for row in cursor.fetchall():
            view = ViewDef(
                name=row[0],
                text=row[1],
                comments=row[2] if row[2] else ""
            )
            views.append(view)

        cursor.close()
        return views

    def extract_sequences(self) -> List[SequenceDef]:
        """Extract all sequence definitions from the schema."""
        schema = self._get_schema()
        sequences = []

        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT sequence_name, min_value, max_value, increment_by, 
                   cycle_flag, order_flag, cache_size, last_number
            FROM all_sequences
            WHERE sequence_owner = :schema
            ORDER BY sequence_name
        """, schema=schema)

        for row in cursor.fetchall():
            seq = SequenceDef(
                name=row[0],
                min_value=row[1],
                max_value=row[2],
                increment_by=row[3],
                cycle_flag=row[4] == 'Y',
                order_flag=row[5] == 'Y',
                cache_size=row[6],
                last_number=row[7]
            )
            sequences.append(seq)

        cursor.close()
        return sequences

    def extract_procedures(self) -> List[ProcedureDef]:
        """Extract all procedure definitions from the schema."""
        schema = self._get_schema()
        procedures = []

        cursor = self.connection.cursor()
        
        # Get distinct procedure/function names
        cursor.execute("""
            SELECT DISTINCT name, type
            FROM all_source
            WHERE owner = :schema AND name NOT LIKE 'BIN$%'
            ORDER BY name
        """, schema=schema)

        objects = cursor.fetchall()

        for obj_name, obj_type in objects:
            cursor.execute("""
                SELECT text
                FROM all_source
                WHERE owner = :schema AND name = :name AND type = :type
                ORDER BY line
            """, schema=schema, name=obj_name, type=obj_type)

            source_lines = [row[0] for row in cursor.fetchall()]
            source = ''.join(source_lines)

            # Get arguments
            cursor.execute("""
                SELECT argument_name, data_type, in_out, position
                FROM all_arguments
                WHERE owner = :schema AND object_name = :name
                ORDER BY position, sequence
            """, schema=schema, name=obj_name)

            args = []
            for row in cursor.fetchall():
                arg_name = row[0] if row[0] else 'RETURN'
                args.append({
                    'name': arg_name,
                    'data_type': row[1],
                    'in_out': row[2],
                    'position': row[3]
                })

            proc = ProcedureDef(
                name=obj_name,
                type=obj_type,
                source=source,
                arguments=args
            )
            procedures.append(proc)

        cursor.close()
        return procedures

    def extract_triggers(self) -> List[TriggerDef]:
        """Extract all trigger definitions from the schema."""
        schema = self._get_schema()
        triggers = []

        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT trigger_name, table_name, triggering_event, 
                   trigger_type, status
            FROM all_triggers
            WHERE owner = :schema
            ORDER BY trigger_name
        """, schema=schema)

        for row in cursor.fetchall():
            cursor.execute("""
                SELECT text
                FROM all_source
                WHERE owner = :schema AND name = :name AND type = 'TRIGGER'
                ORDER BY line
            """, schema=schema, name=row[0])

            source_lines = [r[0] for r in cursor.fetchall()]
            source = ''.join(source_lines)

            trigger = TriggerDef(
                name=row[0],
                table_name=row[1],
                triggering_event=row[2],
                trigger_type=row[3],
                source=source
            )
            triggers.append(trigger)

        cursor.close()
        return triggers

    def extract_all(self) -> Dict:
        """Extract all database objects."""
        self.connect()
        try:
            result = {
                'tables': self.extract_tables(),
                'views': self.extract_views(),
                'sequences': self.extract_sequences(),
                'procedures': self.extract_procedures(),
                'triggers': self.extract_triggers(),
            }
            return result
        finally:
            self.disconnect()
