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
from mssql_converter import DDLConverter, TableConverter, ViewConverter
from oracle_extractor import ColumnDef, IndexDef, ProcedureDef, TableDef


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


if __name__ == "__main__":
    unittest.main()
