"""
Oracle to MSSQL DDL Converter
Main CLI entry point.
"""

import argparse
import os
import sys
import json
from datetime import datetime

from config import Config, OracleConfig, ConversionConfig
from oracle_extractor import OracleExtractor
from mssql_converter import DDLConverter
from mssql_executor import MSSQLExecutor, ExecutionResult


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Oracle to MSSQL DDL Converter - Extract Oracle schema and convert to MSSQL-compatible DDL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --host localhost --service-name ORCL --user scott --password tiger --schema SCOTT
  %(prog)s --config config.json
  %(prog)s --host localhost --service-name ORCL --user scott --password tiger --output ./migration
  %(prog)s --host localhost --sid ORCL --user scott --password tiger --single-file --include-tables --include-views
        """
    )
    
    # Connection options
    conn_group = parser.add_argument_group('Oracle Connection')
    conn_group.add_argument('--host', help='Oracle database host')
    conn_group.add_argument('--port', type=int, help='Oracle database port (default: 1521)')
    conn_group.add_argument('--service-name', help='Oracle service name')
    conn_group.add_argument('--sid', help='Oracle SID')
    conn_group.add_argument('--user', help='Oracle username')
    conn_group.add_argument('--password', help='Oracle password')
    conn_group.add_argument('--schema', help='Schema to extract (default: connected user)')
    
    # Configuration file
    parser.add_argument('--config', help='Path to JSON configuration file')

    # MSSQL connection options
    mssql_group = parser.add_argument_group('MSSQL Connection (for --execute)')
    mssql_group.add_argument('--mssql-host', help='MSSQL server host')
    mssql_group.add_argument('--mssql-port', type=int, help='MSSQL server port (default: 1433)')
    mssql_group.add_argument('--mssql-database', help='MSSQL target database name')
    mssql_group.add_argument('--mssql-user', help='MSSQL username')
    mssql_group.add_argument('--mssql-password', help='MSSQL password')

    # Execute options
    exec_group = parser.add_argument_group('Execution Options')
    exec_group.add_argument(
        '--execute', action='store_true',
        help='Execute converted DDL directly on MSSQL after generation',
    )
    exec_group.add_argument(
        '--dry-run', action='store_true',
        help='Parse DDL statements without executing (use with --execute)',
    )
    exec_group.add_argument(
        '--stop-on-error', action='store_true',
        help='Stop execution on first error (default: continue)',
    )
    
    # Output options
    output_group = parser.add_argument_group('Output Options')
    output_group.add_argument('--output', '-o', help='Output directory (default: ./output)')
    output_group.add_argument('--single-file', action='store_true', default=None, help='Generate single migration script')
    output_group.add_argument('--target-schema', help='Target MSSQL schema (default: dbo)')
    
    # Object type filters
    filter_group = parser.add_argument_group('Object Type Filters')
    filter_group.add_argument('--include-tables', action='store_true', default=None, help='Include tables')
    filter_group.add_argument('--include-views', action='store_true', default=None, help='Include views')
    filter_group.add_argument('--include-sequences', action='store_true', default=None, help='Include sequences')
    filter_group.add_argument('--include-procedures', action='store_true', default=None, help='Include procedures')
    filter_group.add_argument('--include-functions', action='store_true', default=None, help='Include functions')
    filter_group.add_argument('--include-triggers', action='store_true', default=None, help='Include triggers')
    filter_group.add_argument('--include-indexes', action='store_true', default=None, help='Include indexes')
    
    filter_group.add_argument('--exclude-tables', action='store_false', dest='include_tables', help='Exclude tables')
    filter_group.add_argument('--exclude-views', action='store_false', dest='include_views', help='Exclude views')
    filter_group.add_argument('--exclude-sequences', action='store_false', dest='include_sequences', help='Exclude sequences')
    filter_group.add_argument('--exclude-procedures', action='store_false', dest='include_procedures', help='Exclude procedures')
    filter_group.add_argument('--exclude-functions', action='store_false', dest='include_functions', help='Exclude functions')
    filter_group.add_argument('--exclude-triggers', action='store_false', dest='include_triggers', help='Exclude triggers')
    filter_group.add_argument('--exclude-indexes', action='store_false', dest='include_indexes', help='Exclude indexes')
    
    # Conversion options
    conv_group = parser.add_argument_group('Conversion Options')
    conv_group.add_argument('--no-auto-increment', action='store_true', default=None, help='Disable auto-increment conversion')
    conv_group.add_argument('--remove-schema-prefix', action='store_true', default=None, help='Remove Oracle schema prefix')
    conv_group.add_argument('--keep-schema-prefix', action='store_false', dest='remove_schema_prefix', help='Keep Oracle schema prefix')
    conv_group.add_argument('--type-mappings', help='Custom type mappings JSON string')
    
    return parser.parse_args()


def load_config(args) -> Config:
    """Load configuration from file, env, or command line."""
    config = None
    
    if args.config:
        config = Config.from_file(args.config)
    elif os.getenv('ORACLE_HOST') or os.getenv('ORACLE_SERVICE_NAME'):
        config = Config.from_env()
    else:
        config = Config()
    
    # Override with command line arguments
    if args.host:
        config.oracle.host = args.host
    if args.port is not None:
        config.oracle.port = args.port
    if args.service_name:
        config.oracle.service_name = args.service_name
    if args.sid:
        config.oracle.sid = args.sid
    if args.user:
        config.oracle.username = args.user
    if args.password:
        config.oracle.password = args.password
    if args.schema:
        config.oracle.schema = args.schema
    
    if args.output:
        config.conversion.output_directory = args.output
    if args.single_file is not None:
        config.conversion.single_file = args.single_file
    if args.target_schema:
        config.conversion.target_schema = args.target_schema

    # MSSQL connection overrides
    if getattr(args, 'mssql_host', None):
        config.mssql.host = args.mssql_host
    if getattr(args, 'mssql_port', None) is not None:
        config.mssql.port = args.mssql_port
    if getattr(args, 'mssql_database', None):
        config.mssql.database = args.mssql_database
    if getattr(args, 'mssql_user', None):
        config.mssql.username = args.mssql_user
    if getattr(args, 'mssql_password', None):
        config.mssql.password = args.mssql_password

    for option in (
        'include_tables',
        'include_views',
        'include_sequences',
        'include_procedures',
        'include_functions',
        'include_triggers',
        'include_indexes',
        'remove_schema_prefix',
    ):
        value = getattr(args, option)
        if value is not None:
            setattr(config.conversion, option, value)

    if args.no_auto_increment:
        config.conversion.handle_auto_increment = False
    
    if args.type_mappings:
        config.conversion.type_mappings = json.loads(args.type_mappings)
    
    return config


def save_output(converted: dict, config: Config):
    """Save converted DDL to files."""
    output_dir = config.conversion.output_directory
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if config.conversion.single_file:
        # Generate single migration script
        full_script = converted.get('_full_script', '')
        
        output_file = os.path.join(output_dir, f'migration_{timestamp}.sql')
        with open(output_file, 'w', encoding=config.conversion.file_encoding) as f:
            f.write(full_script)
        
        print(f"\nGenerated single migration script: {output_file}")
    else:
        # Generate separate files for each object type
        files_created = []
        
        if converted.get('sequences'):
            seq_file = os.path.join(output_dir, f'01_sequences_{timestamp}.sql')
            with open(seq_file, 'w', encoding=config.conversion.file_encoding) as f:
                f.write('\n'.join(converted['sequences']))
            files_created.append(seq_file)
        
        if converted.get('tables'):
            table_file = os.path.join(output_dir, f'02_tables_{timestamp}.sql')
            with open(table_file, 'w', encoding=config.conversion.file_encoding) as f:
                f.write('\n'.join(converted['tables']))
            files_created.append(table_file)
        
        if converted.get('views'):
            view_file = os.path.join(output_dir, f'03_views_{timestamp}.sql')
            with open(view_file, 'w', encoding=config.conversion.file_encoding) as f:
                f.write('\n'.join(converted['views']))
            files_created.append(view_file)
        
        if converted.get('procedures'):
            proc_file = os.path.join(output_dir, f'04_procedures_{timestamp}.sql')
            with open(proc_file, 'w', encoding=config.conversion.file_encoding) as f:
                f.write('\n'.join(converted['procedures']))
            files_created.append(proc_file)
        
        if converted.get('triggers'):
            trigger_file = os.path.join(output_dir, f'05_triggers_{timestamp}.sql')
            with open(trigger_file, 'w', encoding=config.conversion.file_encoding) as f:
                f.write('\n'.join(converted['triggers']))
            files_created.append(trigger_file)
        
        print(f"\nGenerated {len(files_created)} files:")
        for f in files_created:
            print(f"  - {f}")


def print_summary(extracted: dict, converted: dict):
    """Print extraction and conversion summary."""
    print("\n" + "=" * 60)
    print("Oracle to MSSQL Migration Summary")
    print("=" * 60)

    print("\nExtracted Objects:")
    print(f"  Tables:     {len(extracted.get('tables', []))}")
    print(f"  Views:      {len(extracted.get('views', []))}")
    print(f"  Sequences:  {len(extracted.get('sequences', []))}")
    print(f"  Procedures: {len(extracted.get('procedures', []))}")
    print(f"  Triggers:   {len(extracted.get('triggers', []))}")

    total_extracted = (
        len(extracted.get('tables', [])) +
        len(extracted.get('views', [])) +
        len(extracted.get('sequences', [])) +
        len(extracted.get('procedures', [])) +
        len(extracted.get('triggers', []))
    )

    total_converted = (
        len(converted.get('tables', [])) +
        len(converted.get('views', [])) +
        len(converted.get('sequences', [])) +
        len(converted.get('procedures', [])) +
        len(converted.get('triggers', []))
    )

    print(f"\nTotal extracted: {total_extracted}")
    print(f"Total converted: {total_converted}")

    if total_extracted > 0:
        compatibility = (total_converted / total_extracted) * 100
        print(f"Compatibility:   {compatibility:.1f}%")

    print("=" * 60)


def execute_on_mssql(config: Config, converted: dict, args) -> None:
    """Execute converted DDL on MSSQL database.

    Args:
        config: Full configuration including MSSQL connection details.
        converted: Dictionary of converted DDL strings by object type.
        args: Parsed command-line arguments.
    """
    mssql = config.mssql

    # Validate MSSQL connection
    if not mssql.host:
        print("Error: --mssql-host is required when using --execute")
        sys.exit(1)
    if not mssql.database:
        print("Error: --mssql-database is required when using --execute")
        sys.exit(1)
    if not mssql.username:
        print("Error: --mssql-user is required when using --execute")
        sys.exit(1)
    if mssql.password is None:
        print("Error: --mssql-password is required when using --execute")
        sys.exit(1)

    mode_label = " (DRY RUN)" if args.dry_run else ""
    print(f"\n{'=' * 60}")
    print(f"Executing DDL on MSSQL: {mssql.host}:{mssql.port}/{mssql.database}{mode_label}")
    print(f"{'=' * 60}")

    executor = MSSQLExecutor(mssql)

    try:
        if config.conversion.single_file:
            script = converted.get('_full_script', '')
            if script:
                result = executor.execute_script(script, dry_run=args.dry_run)
                _print_execution_result(result)
        else:
            # Execute in order: sequences -> tables -> views -> procedures -> triggers
            ordered = [
                ('Sequences', converted.get('sequences', [])),
                ('Tables', converted.get('tables', [])),
                ('Views', converted.get('views', [])),
                ('Procedures', converted.get('procedures', [])),
                ('Triggers', converted.get('triggers', [])),
            ]
            total_result = ExecutionResult()
            for label, items in ordered:
                if not items:
                    continue
                print(f"\n--- {label} ---")
                script = '\n'.join(items)
                result = executor.execute_script(script, dry_run=args.dry_run)
                total_result.total_statements += result.total_statements
                total_result.succeeded += result.succeeded
                total_result.failed += result.failed
                total_result.skipped += result.skipped
                total_result.errors.extend(result.errors)
                _print_execution_result(result)

                if args.stop_on_error and result.failed > 0:
                    print("\nStopped due to --stop-on-error flag")
                    break

            if total_result.total_statements > 0:
                print(f"\n{'=' * 60}")
                print(f"Overall Result:")
                _print_execution_result(total_result)

    finally:
        executor.close()


def _print_execution_result(result) -> None:
    """Print formatted execution result."""
    print(f"  Statements: {result.total_statements}")
    print(f"  Succeeded:  {result.succeeded}")
    print(f"  Failed:     {result.failed}")
    print(f"  Skipped:    {result.skipped}")
    print(f"  Success:    {result.success_rate:.1f}%")

    if result.errors:
        print(f"\n  Errors ({len(result.errors)}):")
        for err in result.errors[:10]:  # Show first 10 errors
            print(f"    [{err['index']}] {err['type']}: {err['error'][:120]}")
        if len(result.errors) > 10:
            print(f"    ... and {len(result.errors) - 10} more")


def main():
    """Main entry point."""
    args = parse_args()
    
    # Load configuration
    config = load_config(args)
    
    # Validate required configuration
    if not config.oracle.host:
        print("Error: Oracle host is required. Use --host or --config option.")
        sys.exit(1)
    
    if not config.oracle.username:
        print("Error: Oracle username is required. Use --user or --config option.")
        sys.exit(1)
    
    if not config.oracle.password:
        print("Error: Oracle password is required. Use --password or --config option.")
        sys.exit(1)
    
    if not config.oracle.service_name and not config.oracle.sid:
        print("Error: Either --service-name or --sid is required.")
        sys.exit(1)
    
    print(f"\nConnecting to Oracle database at {config.oracle.dsn}")
    print(f"Schema: {config.oracle.schema or '(connected user)'}")
    print(f"Target MSSQL schema: {config.conversion.target_schema}")
    print()
    
    try:
        # Extract Oracle DDL
        extractor = OracleExtractor(config.oracle)
        extracted = extractor.extract_all()
        
        print(f"Extraction complete.")
        
        # Convert to MSSQL
        converter = DDLConverter(config.conversion)
        converted = converter.convert_all(extracted)
        
        # Generate full script for single-file mode
        if config.conversion.single_file:
            converted['_full_script'] = converter.generate_full_script(converted=converted)
        
        # Print summary
        print_summary(extracted, converted)

        # Save output
        save_output(converted, config)

        print("\nMigration script generation complete!")

        # Execute on MSSQL if requested
        if args.execute:
            execute_on_mssql(config, converted, args)

        # Print warnings for manual review items
        print("\n" + "-" * 60)
        print("Manual Review Recommendations:")
        print("-" * 60)
        print("1. Review CONNECT BY hierarchical queries - may need CTE conversion")
        print("2. Review complex DECODE expressions - may need CASE conversion")
        print("3. Review triggers with :NEW/:OLD references")
        print("4. Review stored procedures with Oracle-specific packages")
        print("5. Test all sequences with NEXTVAL usage")
        print("6. Verify CLOB/BLOB data type conversions")
        print("7. Review date/time function conversions")
        print("-" * 60)
        
    except Exception as e:
        print(f"\nError during migration: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
