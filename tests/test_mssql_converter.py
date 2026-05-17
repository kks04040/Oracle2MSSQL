"""Regression tests for Oracle to MSSQL DDL conversion."""

import sys
import tempfile
import types
import unittest
from argparse import Namespace
from pathlib import Path


sys.modules.setdefault("oracledb", types.SimpleNamespace())

from config import ConversionConfig
from main import load_config
from mssql_converter import DDLConverter, ProcedureConverter, TableConverter, ViewConverter
from oracle_extractor import ColumnDef, ConstraintDef, IndexDef, ProcedureDef, TableDef


class ViewConverterTests(unittest.TestCase):
    """Tests for view SQL conversion."""

    def test_rownum_is_not_added_when_missing(self):
        converter = ViewConverter(ConversionConfig())

        sql = converter._convert_rownum("SELECT ID, NAME FROM USERS")

        self.assertEqual(sql, "SELECT ID, NAME FROM USERS")

    def test_rownum_where_condition_becomes_top(self):
        converter = ViewConverter(ConversionConfig())

        sql = converter._convert_rownum("SELECT ID FROM USERS WHERE ROWNUM <= 5 AND ACTIVE = 1")

        self.assertEqual(sql, "SELECT TOP 5 ID FROM USERS WHERE ACTIVE = 1")

    def test_rownum_and_condition_becomes_top(self):
        converter = ViewConverter(ConversionConfig())

        sql = converter._convert_rownum("SELECT ID FROM USERS WHERE ACTIVE = 1 AND ROWNUM = 1")

        self.assertEqual(sql, "SELECT TOP 1 ID FROM USERS WHERE ACTIVE = 1")

    def test_schema_prefix_removal_keeps_alias_column_references(self):
        converter = ViewConverter(ConversionConfig(remove_schema_prefix=True))

        sql = converter._convert_view_body(
            "SELECT T.ID FROM APP.USERS T JOIN APP.ORDERS O ON O.USER_ID = T.ID"
        )

        self.assertIn("FROM USERS T", sql)
        self.assertIn("JOIN ORDERS O", sql)
        self.assertIn("O.USER_ID = T.ID", sql)


class TableConverterTests(unittest.TestCase):
    """Tests for table conversion options."""

    def test_include_indexes_false_suppresses_non_constraint_indexes(self):
        config = ConversionConfig(include_indexes=False)
        table = TableDef(
            name="USERS",
            columns=[ColumnDef(name="ID", data_type="NUMBER", data_precision=10, nullable=False)],
            indexes=[IndexDef(name="IX_USERS_ID", table_name="USERS", columns=["ID"])],
        )

        ddl = TableConverter(config).convert(table)

        self.assertNotIn("CREATE INDEX", ddl)

    def test_composite_primary_key_does_not_create_multiple_identity_columns(self):
        config = ConversionConfig(handle_auto_increment=True)
        table = TableDef(
            name="ORDER_ITEM",
            columns=[
                ColumnDef(name="ORDER_ID", data_type="NUMBER", data_precision=10, nullable=False),
                ColumnDef(name="ITEM_ID", data_type="NUMBER", data_precision=10, nullable=False),
            ],
            constraints=[
                ConstraintDef(name="PK_ORDER_ITEM", type="P", columns=["ORDER_ID", "ITEM_ID"]),
            ],
        )

        ddl = TableConverter(config).convert(table)

        self.assertEqual(ddl.count("IDENTITY(1,1)"), 0)


class DDLConverterTests(unittest.TestCase):
    """Tests for top-level conversion filters."""

    def test_function_filter_is_independent_from_procedure_filter(self):
        config = ConversionConfig(include_procedures=False, include_functions=True)
        extracted = {
            "procedures": [
                ProcedureDef(name="DO_WORK", type="PROCEDURE", source="CREATE PROCEDURE DO_WORK AS BEGIN NULL; END;"),
                ProcedureDef(name="GET_VALUE", type="FUNCTION", source="CREATE FUNCTION GET_VALUE RETURN NUMBER AS BEGIN RETURN 1; END;"),
            ]
        }

        result = DDLConverter(config).convert_all(extracted)
        ddl = "\n".join(result["procedures"])

        self.assertNotIn("DO_WORK", ddl)
        self.assertIn("GET_VALUE", ddl)


class ProcedureConverterTests(unittest.TestCase):
    """Tests for procedure/function conversion details."""

    def test_function_returns_clause_uses_return_argument_type(self):
        converter = ProcedureConverter(ConversionConfig())
        proc = ProcedureDef(
            name="GET_VALUE",
            type="FUNCTION",
            source="CREATE OR REPLACE FUNCTION GET_VALUE(P_ID NUMBER) RETURN NUMBER AS BEGIN RETURN 1; END;",
            arguments=[
                {"name": "RETURN", "data_type": "NUMBER", "in_out": "OUT", "position": 0},
                {"name": "P_ID", "data_type": "NUMBER", "in_out": "IN", "position": 1},
            ],
        )

        ddl = converter.convert(proc)

        self.assertIn("RETURNS DECIMAL(18,2)", ddl)


class ConfigLoadingTests(unittest.TestCase):
    """Tests for configuration precedence."""

    def test_config_file_values_are_not_overwritten_by_unspecified_cli_defaults(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            config_path.write_text(
                """
                {
                    "oracle": {
                        "host": "db.example.com",
                        "service_name": "ORCL",
                        "username": "scott",
                        "password": "tiger"
                    },
                    "conversion": {
                        "output_directory": "./migration",
                        "single_file": true,
                        "target_schema": "app",
                        "include_indexes": false,
                        "remove_schema_prefix": false
                    }
                }
                """,
                encoding="utf-8",
            )

            args = Namespace(
                config=str(config_path),
                host=None,
                port=None,
                service_name=None,
                sid=None,
                user=None,
                password=None,
                schema=None,
                output=None,
                single_file=None,
                target_schema=None,
                include_tables=None,
                include_views=None,
                include_sequences=None,
                include_procedures=None,
                include_functions=None,
                include_triggers=None,
                include_indexes=None,
                remove_schema_prefix=None,
                no_auto_increment=None,
                type_mappings=None,
            )

            config = load_config(args)

        self.assertEqual(config.conversion.output_directory, "./migration")
        self.assertTrue(config.conversion.single_file)
        self.assertEqual(config.conversion.target_schema, "app")
        self.assertFalse(config.conversion.include_indexes)
        self.assertFalse(config.conversion.remove_schema_prefix)


class DecodeConversionTests(unittest.TestCase):
    """Tests for DECODE -> CASE conversion."""

    def test_decode_basic(self):
        from mssql_converter import _convert_decode
        sql = "SELECT DECODE(status, 1, 'Active', 0, 'Inactive', 'Unknown') FROM users"
        result = _convert_decode(sql)
        self.assertIn("CASE WHEN status=1 THEN 'Active' WHEN status=0 THEN 'Inactive' ELSE 'Unknown' END", result)

    def test_decode_single_pair(self):
        from mssql_converter import _convert_decode
        sql = "SELECT DECODE(x, 1, 'one') FROM t"
        result = _convert_decode(sql)
        self.assertIn("CASE WHEN x=1 THEN 'one' ELSE NULL END", result)

    def test_decode_nested_function(self):
        from mssql_converter import _convert_decode
        sql = "SELECT DECODE(type, 'A', NVL(name, 'default'), 'B', 'other') FROM t"
        result = _convert_decode(sql)
        self.assertIn("CASE WHEN type='A' THEN NVL(name, 'default') WHEN type='B' THEN 'other' ELSE NULL END", result)

    def test_decode_not_enough_args(self):
        from mssql_converter import _convert_decode
        sql = "SELECT DECODE(x) FROM t"
        result = _convert_decode(sql)
        self.assertEqual(result, sql)  # Should remain unchanged


class DateFunctionConversionTests(unittest.TestCase):
    """Tests for Oracle date function conversions."""

    def test_add_months(self):
        from mssql_converter import _convert_date_functions
        sql = "SELECT ADD_MONTHS(hire_date, 3) FROM emp"
        result = _convert_date_functions(sql)
        self.assertEqual(result, "SELECT DATEADD(month, 3, hire_date) FROM emp")

    def test_months_between(self):
        from mssql_converter import _convert_date_functions
        sql = "SELECT MONTHS_BETWEEN(end_date, start_date) FROM projects"
        result = _convert_date_functions(sql)
        self.assertEqual(result, "SELECT DATEDIFF(month, start_date, end_date) FROM projects")

    def test_last_day(self):
        from mssql_converter import _convert_date_functions
        sql = "SELECT LAST_DAY(order_date) FROM orders"
        result = _convert_date_functions(sql)
        self.assertEqual(result, "SELECT EOMONTH(order_date) FROM orders")

    def test_trunc_year(self):
        from mssql_converter import _convert_date_functions
        sql = "SELECT TRUNC(order_date, 'YYYY') FROM orders"
        result = _convert_date_functions(sql)
        self.assertEqual(result, "SELECT DATEFROMPARTS(YEAR(order_date), 1, 1) FROM orders")

    def test_trunc_month(self):
        from mssql_converter import _convert_date_functions
        sql = "SELECT TRUNC(order_date, 'MM') FROM orders"
        result = _convert_date_functions(sql)
        self.assertEqual(result, "SELECT DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1) FROM orders")

    def test_trunc_quarter(self):
        from mssql_converter import _convert_date_functions
        sql = "SELECT TRUNC(order_date, 'Q') FROM orders"
        result = _convert_date_functions(sql)
        self.assertIn("DATEFROMPARTS(YEAR", result)

    def test_trunc_single_arg(self):
        from mssql_converter import _convert_date_functions
        sql = "SELECT TRUNC(order_date) FROM orders"
        result = _convert_date_functions(sql)
        self.assertEqual(result, "SELECT CAST(order_date AS DATE) FROM orders")

    def test_next_day_marked_for_review(self):
        from mssql_converter import _convert_date_functions
        sql = "SELECT NEXT_DAY(SYSDATE, 'MONDAY') FROM dual"
        result = _convert_date_functions(sql)
        self.assertIn("NEXT_DAY conversion:", result)
        self.assertIn("manually", result)


class OtherFunctionConversionTests(unittest.TestCase):
    """Tests for miscellaneous Oracle function conversions."""

    def test_nvl2(self):
        from mssql_converter import _convert_other_functions
        sql = "SELECT NVL2(col, 'has value', 'no value') FROM t"
        result = _convert_other_functions(sql)
        self.assertIn("CASE WHEN col IS NOT NULL THEN 'has value' ELSE 'no value' END", result)

    def test_sys_guid(self):
        from mssql_converter import _convert_other_functions
        sql = "SELECT SYS_GUID() FROM dual"
        result = _convert_other_functions(sql)
        self.assertEqual(result, "SELECT NEWID() FROM dual")

    def test_ln_to_log(self):
        from mssql_converter import _convert_other_functions
        sql = "SELECT LN(value) FROM t"
        result = _convert_other_functions(sql)
        self.assertEqual(result, "SELECT LOG(value) FROM t")

    def test_mod_to_percent(self):
        from mssql_converter import _convert_other_functions
        sql = "SELECT MOD(a, b) FROM t"
        result = _convert_other_functions(sql)
        self.assertEqual(result, "SELECT (a % b) FROM t")

    def test_bitand(self):
        from mssql_converter import _convert_other_functions
        sql = "SELECT BITAND(flags, 1) FROM t"
        result = _convert_other_functions(sql)
        self.assertEqual(result, "SELECT (flags & 1) FROM t")

    def test_initcap_marked_for_review(self):
        from mssql_converter import _convert_string_functions
        sql = "SELECT INITCAP(name) FROM t"
        result = _convert_string_functions(sql)
        self.assertIn("INITCAP:", result)
        self.assertIn("scalar function", result)


class StringFunctionConversionTests(unittest.TestCase):
    """Tests for Oracle string function conversions."""

    def test_length_to_len(self):
        from mssql_converter import _convert_string_functions
        sql = "SELECT LENGTH(name) FROM users"
        result = _convert_string_functions(sql)
        self.assertEqual(result, "SELECT LEN(name) FROM users")

    def test_substr_with_expressions(self):
        from mssql_converter import _convert_string_functions
        sql = "SELECT SUBSTR(name, 1, LENGTH(name)-1) FROM t"
        result = _convert_string_functions(sql)
        self.assertIn("SUBSTRING(name, 1, LEN(name)-1)", result)

    def test_instr_two_args(self):
        from mssql_converter import _convert_string_functions
        sql = "SELECT INSTR(str, 'abc') FROM t"
        result = _convert_string_functions(sql)
        self.assertEqual(result, "SELECT CHARINDEX('abc', str) FROM t")

    def test_instr_three_args(self):
        from mssql_converter import _convert_string_functions
        sql = "SELECT INSTR(str, 'abc', 5) FROM t"
        result = _convert_string_functions(sql)
        self.assertEqual(result, "SELECT CHARINDEX('abc', str, 5) FROM t")

    def test_instr_four_args_marked(self):
        from mssql_converter import _convert_string_functions
        sql = "SELECT INSTR(str, 'abc', 1, 2) FROM t"
        result = _convert_string_functions(sql)
        self.assertIn("INSTR with occurrence:", result)

    def test_to_char_with_format_marked(self):
        from mssql_converter import _convert_string_functions
        sql = "SELECT TO_CHAR(date_col, 'YYYY-MM-DD') FROM t"
        result = _convert_string_functions(sql)
        self.assertIn("TO_CHAR with format:", result)

    def test_to_date_with_format_marked(self):
        from mssql_converter import _convert_string_functions
        sql = "SELECT TO_DATE('2023-01-01', 'YYYY-MM-DD') FROM dual"
        result = _convert_string_functions(sql)
        self.assertIn("TO_DATE with format:", result)


class AggregateFunctionConversionTests(unittest.TestCase):
    """Tests for Oracle aggregate function conversions."""

    def test_wm_concat(self):
        from mssql_converter import _convert_aggregate_functions
        sql = "SELECT WM_CONCAT(name) FROM users"
        result = _convert_aggregate_functions(sql)
        self.assertIn("STRING_AGG(name, ',')", result)

    def test_stddev(self):
        from mssql_converter import _convert_aggregate_functions
        sql = "SELECT STDDEV(salary) FROM emp"
        result = _convert_aggregate_functions(sql)
        self.assertEqual(result, "SELECT STDEV(salary) FROM emp")

    def test_variance(self):
        from mssql_converter import _convert_aggregate_functions
        sql = "SELECT VARIANCE(salary) FROM emp"
        result = _convert_aggregate_functions(sql)
        self.assertEqual(result, "SELECT VAR(salary) FROM emp")

    def test_listagg_with_within_group(self):
        from mssql_converter import _convert_aggregate_functions
        sql = "SELECT LISTAGG(name, ',') WITHIN GROUP (ORDER BY name) FROM emp"
        result = _convert_aggregate_functions(sql)
        self.assertIn("STRING_AGG(name, ',') WITHIN GROUP (ORDER BY name)", result)


class RegexFunctionConversionTests(unittest.TestCase):
    """Tests for Oracle REGEXP_* function handling."""

    def test_regexp_like_marked(self):
        from mssql_converter import _convert_regex_functions
        sql = "SELECT * FROM t WHERE REGEXP_LIKE(col, '^[A-Z]')"
        result = _convert_regex_functions(sql)
        self.assertIn("REGEXP_LIKE(", result)
        self.assertIn("manually", result)

    def test_regexp_substr_marked(self):
        from mssql_converter import _convert_regex_functions
        sql = "SELECT REGEXP_SUBSTR(text, '[0-9]+') FROM t"
        result = _convert_regex_functions(sql)
        self.assertIn("REGEXP_SUBSTR:", result)

    def test_regexp_replace_marked(self):
        from mssql_converter import _convert_regex_functions
        sql = "SELECT REGEXP_REPLACE(text, 'old', 'new') FROM t"
        result = _convert_regex_functions(sql)
        self.assertIn("REGEXP_REPLACE:", result)


class DbmsFunctionConversionTests(unittest.TestCase):
    """Tests for Oracle DBMS_* package handling."""

    def test_dbms_output_put_line(self):
        from mssql_converter import _convert_dbms_functions
        sql = "DBMS_OUTPUT.PUT_LINE('Hello')"
        result = _convert_dbms_functions(sql)
        self.assertEqual(result, "PRINT 'Hello'")

    def test_dbms_random_value(self):
        from mssql_converter import _convert_dbms_functions
        sql = "SELECT DBMS_RANDOM.VALUE() FROM dual"
        result = _convert_dbms_functions(sql)
        self.assertEqual(result, "SELECT RAND() FROM dual")

    def test_dbms_random_value_range(self):
        from mssql_converter import _convert_dbms_functions
        sql = "SELECT DBMS_RANDOM.VALUE(1, 100) FROM dual"
        result = _convert_dbms_functions(sql)
        self.assertIn("RAND()", result)

    def test_generic_dbms_marked(self):
        from mssql_converter import _convert_dbms_functions
        sql = "result := DBMS_RANDOM.VALUE(1, 100);"
        result = _convert_dbms_functions(sql)
        self.assertIn("RAND()", result)


class ConnectByConversionTests(unittest.TestCase):
    """Tests for CONNECT BY -> recursive CTE conversion."""

    def test_connect_by_basic(self):
        converter = ViewConverter(ConversionConfig())
        sql = "SELECT id, name, parent_id FROM org CONNECT BY PRIOR id = parent_id"
        result = converter._convert_connect_by(sql)
        self.assertIn("WITH cte_hierarchy", result)
        self.assertIn("UNION ALL", result)
        self.assertIn("SELECT", result)

    def test_connect_by_with_start_with(self):
        converter = ViewConverter(ConversionConfig())
        sql = "SELECT id, name FROM org START WITH parent_id IS NULL CONNECT BY PRIOR id = parent_id"
        result = converter._convert_connect_by(sql)
        self.assertIn("WITH cte_hierarchy", result)
        self.assertIn("UNION ALL", result)

    def test_connect_by_no_match_returns_original(self):
        converter = ViewConverter(ConversionConfig())
        sql = "SELECT * FROM users WHERE id = 1"
        result = converter._convert_connect_by(sql)
        self.assertEqual(result, sql)


class ViewConverterIntegrationTests(unittest.TestCase):
    """Integration tests for ViewConverter._convert_view_body with new functions."""

    def test_decode_in_view_body(self):
        converter = ViewConverter(ConversionConfig())
        sql = "SELECT DECODE(status, 1, 'Active', 'Inactive') AS status_label FROM users"
        result = converter._convert_view_body(sql)
        self.assertIn("CASE WHEN status=1 THEN 'Active' ELSE 'Inactive' END", result)

    def test_add_months_in_view_body(self):
        converter = ViewConverter(ConversionConfig())
        sql = "SELECT ADD_MONTHS(order_date, 6) FROM orders"
        result = converter._convert_view_body(sql)
        self.assertIn("DATEADD(month, 6, order_date)", result)

    def test_last_day_in_view_body(self):
        converter = ViewConverter(ConversionConfig())
        sql = "SELECT LAST_DAY(order_date) FROM orders"
        result = converter._convert_view_body(sql)
        self.assertIn("EOMONTH(order_date)", result)

    def test_nvl2_in_view_body(self):
        converter = ViewConverter(ConversionConfig())
        sql = "SELECT NVL2(comments, 'Has notes', 'Empty') FROM t"
        result = converter._convert_view_body(sql)
        self.assertIn("CASE WHEN comments IS NOT NULL THEN 'Has notes' ELSE 'Empty' END", result)

    def test_sys_guid_in_view_body(self):
        converter = ViewConverter(ConversionConfig())
        sql = "SELECT SYS_GUID() AS new_id FROM dual"
        result = converter._convert_view_body(sql)
        self.assertIn("NEWID()", result)

    def test_length_to_len_in_view_body(self):
        converter = ViewConverter(ConversionConfig())
        sql = "SELECT LENGTH(name) FROM users"
        result = converter._convert_view_body(sql)
        self.assertIn("LEN(name)", result)

    def test_stddev_in_view_body(self):
        converter = ViewConverter(ConversionConfig())
        sql = "SELECT STDDEV(salary) FROM emp"
        result = converter._convert_view_body(sql)
        self.assertIn("STDEV(salary)", result)

    def test_regexp_substr_marked_in_view_body(self):
        converter = ViewConverter(ConversionConfig())
        sql = "SELECT REGEXP_SUBSTR(text, '[0-9]+') FROM t"
        result = converter._convert_view_body(sql)
        self.assertIn("REGEXP_SUBSTR:", result)

    def test_dbms_output_in_view_body(self):
        converter = ViewConverter(ConversionConfig())
        sql = "BEGIN DBMS_OUTPUT.PUT_LINE('test'); END;"
        result = converter._convert_view_body(sql)
        self.assertIn("PRINT 'test'", result)

    def test_combined_conversions_in_view_body(self):
        converter = ViewConverter(ConversionConfig())
        sql = """
        SELECT
            DECODE(status, 1, 'Active', 'Inactive') AS status_label,
            ADD_MONTHS(start_date, 3) AS end_date,
            NVL2(comments, 'Has notes', 'Empty') AS has_comments,
            SYS_GUID() AS row_id,
            LENGTH(name) AS name_len
        FROM employees
        WHERE status = 1
        """
        result = converter._convert_view_body(sql)
        self.assertIn("CASE WHEN status=1 THEN 'Active' ELSE 'Inactive' END", result)
        self.assertIn("DATEADD(month, 3, start_date)", result)
        self.assertIn("IS NOT NULL", result)
        self.assertIn("NEWID()", result)
        self.assertIn("LEN(name)", result)


class ProcedureConverterIntegrationTests(unittest.TestCase):
    """Integration tests for ProcedureConverter._convert_source with new functions."""

    def test_decode_in_procedure(self):
        converter = ProcedureConverter(ConversionConfig())
        source = """CREATE OR REPLACE PROCEDURE get_status(p_id NUMBER) AS
        BEGIN
            v_status := DECODE(p_type, 1, 'Active', 'Inactive');
        END;"""
        result = converter._convert_source(source, [], 'PROCEDURE')
        self.assertIn("CASE WHEN p_type=1 THEN 'Active' ELSE 'Inactive' END", result)

    def test_date_functions_in_procedure(self):
        converter = ProcedureConverter(ConversionConfig())
        source = """CREATE OR REPLACE PROCEDURE calc_dates AS
        BEGIN
            v_end := ADD_MONTHS(v_start, 6);
            v_last := LAST_DAY(v_date);
        END;"""
        result = converter._convert_source(source, [], 'PROCEDURE')
        self.assertIn("DATEADD(month, 6, v_start)", result)
        self.assertIn("EOMONTH(v_date)", result)


if __name__ == "__main__":
    unittest.main()
