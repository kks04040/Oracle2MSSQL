# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

Oracle to MSSQL DDL Converter - a CLI tool that extracts schema objects from an Oracle database and converts them to MSSQL-compatible DDL (CREATE TABLE, CREATE VIEW, CREATE PROCEDURE, etc.).

## Structure

```
main.py            # CLI entry point (argparse, orchestration)
config.py          # Dataclass configs: OracleConfig, MSSQLConfig, ConversionConfig, Config
oracle_extractor.py # Connects to Oracle via oracledb, extracts DDL metadata from data dictionary views
mssql_converter.py # Converts extracted Oracle objects to MSSQL DDL (one class per object type)
```

## Architecture

The tool follows a three-phase pipeline:

1. **Config** (`config.py`) - `Config` aggregates `OracleConfig`, `MSSQLConfig`, `ConversionConfig` dataclasses. Supports loading from JSON file, environment variables, or CLI args (CLI overrides all).

2. **Extract** (`oracle_extractor.py`) - `OracleExtractor` connects to Oracle and queries data dictionary views (`all_tables`, `all_tab_columns`, `all_constraints`, `all_views`, `all_sequences`, `all_source`, `all_triggers`) to build Python dataclass objects (`TableDef`, `ViewDef`, `SequenceDef`, `ProcedureDef`, `TriggerDef`).

3. **Convert** (`mssql_converter.py`) - `DDLConverter` orchestrates type-specific converters (`TableConverter`, `ViewConverter`, `SequenceConverter`, `ProcedureConverter`, `TriggerConverter`). Each converter takes an Oracle dataclass and returns a MSSQL DDL string. Conversion uses regex-based text transformation for Oracle function/keyword substitution (e.g., SYSDATE -> GETDATE, NVL -> ISNULL, || -> +, CONNECT BY -> CTE marker).

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run with CLI args
python main.py --host localhost --service-name ORCL --user scott --password tiger --schema SCOTT

# Run with config file
python main.py --config config.json

# Run with environment variables
export ORACLE_HOST=localhost ORACLE_SERVICE_NAME=ORCL ORACLE_USERNAME=scott ORACLE_PASSWORD=tiger ORACLE_SCHEMA=SCOTT
python main.py

# Single output file mode
python main.py --host localhost --service-name ORCL --user scott --password tiger --single-file

# Exclude specific object types
python main.py --host localhost --service-name ORCL --user scott --password tiger --exclude-procedures --exclude-triggers

# Custom output directory and target schema
python main.py --host localhost --service-name ORCL --user scott --password tiger --output ./migration --target-schema dbo
```

## Key Implementation Details

- **No external linter/formatter configured**. Simple style: 4-space indent, docstrings on modules and top-level functions/classes.
- **Single dependency**: `oracledb>=2.0.0` (Thin mode - no Oracle client needed).
- **Output**: Either separate files per object type (`01_sequences_*.sql`, `02_tables_*.sql`, etc.) or a single combined migration script.
- **Conversion coverage**: Tables/views ~95%, sequences ~100%, procedures ~80%, triggers ~85%. CONNECT BY, complex DECODE, and Oracle packages require manual review.
- **`mssql_converter.py`** is the largest file (~900 lines) and contains all conversion logic. Each converter class is self-contained and stateless except for config/sequences references.
- **`oracle_extractor.py`** uses parameterized queries with `:schema`, `:table`, `:constraint` bind variables.
- Config file: `example_config.json` - copy to `config.json` (gitignored) to edit.
