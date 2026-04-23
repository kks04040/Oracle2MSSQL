"""
Oracle to MSSQL DDL Converter
Configuration module for database connection settings.
"""

import os
import json
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class OracleConfig:
    """Oracle database connection configuration."""
    host: str = ""
    port: int = 1521
    service_name: str = ""
    sid: str = ""
    username: str = ""
    password: str = ""
    schema: str = ""

    @property
    def dsn(self) -> str:
        """Create DSN string for cx_Oracle/oracledb."""
        if self.service_name:
            return f"{self.host}:{self.port}/{self.service_name}"
        elif self.sid:
            return f"{self.host}:{self.port}:{self.sid}"
        else:
            return f"{self.host}:{self.port}"


@dataclass
class MSSQLConfig:
    """MSSQL database connection configuration (for output generation)."""
    host: str = ""
    port: int = 1433
    database: str = ""
    username: str = ""
    password: str = ""


@dataclass
class ConversionConfig:
    """DDL conversion configuration."""
    # Data type mapping overrides
    type_mappings: dict = field(default_factory=dict)
    
    # Include/exclude object types
    include_tables: bool = True
    include_views: bool = True
    include_sequences: bool = True
    include_procedures: bool = True
    include_functions: bool = True
    include_triggers: bool = True
    include_indexes: bool = True
    
    # Schema prefix handling
    remove_schema_prefix: bool = True
    target_schema: str = "dbo"
    
    # Output options
    output_directory: str = "./output"
    single_file: bool = False
    file_encoding: str = "utf-8"
    
    # Compatibility options
    convert_nvarchar: bool = True
    convert_number_to_decimal: bool = True
    convert_clob_to_ntext: bool = True
    convert_blob_to_varbinary: bool = True
    handle_auto_increment: bool = True
    convert_sysdate_to_getdate: bool = True
    convert_dual_table: bool = True


@dataclass
class Config:
    """Main configuration."""
    oracle: OracleConfig = field(default_factory=OracleConfig)
    mssql: MSSQLConfig = field(default_factory=MSSQLConfig)
    conversion: ConversionConfig = field(default_factory=ConversionConfig)
    
    @classmethod
    def from_file(cls, filepath: str) -> 'Config':
        """Load configuration from JSON file."""
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        config = cls()
        
        if 'oracle' in data:
            oracle_data = data['oracle']
            config.oracle = OracleConfig(
                host=oracle_data.get('host', ''),
                port=int(oracle_data.get('port', 1521)),
                service_name=oracle_data.get('service_name', ''),
                sid=oracle_data.get('sid', ''),
                username=oracle_data.get('username', ''),
                password=oracle_data.get('password', ''),
                schema=oracle_data.get('schema', '')
            )
        
        if 'mssql' in data:
            mssql_data = data['mssql']
            config.mssql = MSSQLConfig(
                host=mssql_data.get('host', ''),
                port=int(mssql_data.get('port', 1433)),
                database=mssql_data.get('database', ''),
                username=mssql_data.get('username', ''),
                password=mssql_data.get('password', '')
            )
        
        if 'conversion' in data:
            conv_data = data['conversion']
            config.conversion = ConversionConfig(
                type_mappings=conv_data.get('type_mappings', {}),
                include_tables=conv_data.get('include_tables', True),
                include_views=conv_data.get('include_views', True),
                include_sequences=conv_data.get('include_sequences', True),
                include_procedures=conv_data.get('include_procedures', True),
                include_functions=conv_data.get('include_functions', True),
                include_triggers=conv_data.get('include_triggers', True),
                include_indexes=conv_data.get('include_indexes', True),
                remove_schema_prefix=conv_data.get('remove_schema_prefix', True),
                target_schema=conv_data.get('target_schema', 'dbo'),
                output_directory=conv_data.get('output_directory', './output'),
                single_file=conv_data.get('single_file', False),
                file_encoding=conv_data.get('file_encoding', 'utf-8'),
                convert_nvarchar=conv_data.get('convert_nvarchar', True),
                convert_number_to_decimal=conv_data.get('convert_number_to_decimal', True),
                convert_clob_to_ntext=conv_data.get('convert_clob_to_ntext', True),
                convert_blob_to_varbinary=conv_data.get('convert_blob_to_varbinary', True),
                handle_auto_increment=conv_data.get('handle_auto_increment', True),
                convert_sysdate_to_getdate=conv_data.get('convert_sysdate_to_getdate', True),
                convert_dual_table=conv_data.get('convert_dual_table', True)
            )
        
        return config
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables."""
        config = cls()
        
        config.oracle.host = os.getenv('ORACLE_HOST', '')
        config.oracle.port = int(os.getenv('ORACLE_PORT', '1521'))
        config.oracle.service_name = os.getenv('ORACLE_SERVICE_NAME', '')
        config.oracle.sid = os.getenv('ORACLE_SID', '')
        config.oracle.username = os.getenv('ORACLE_USERNAME', '')
        config.oracle.password = os.getenv('ORACLE_PASSWORD', '')
        config.oracle.schema = os.getenv('ORACLE_SCHEMA', '')
        
        config.mssql.host = os.getenv('MSSQL_HOST', '')
        config.mssql.port = int(os.getenv('MSSQL_PORT', '1433'))
        config.mssql.database = os.getenv('MSSQL_DATABASE', '')
        config.mssql.username = os.getenv('MSSQL_USERNAME', '')
        config.mssql.password = os.getenv('MSSQL_PASSWORD', '')
        
        config.conversion.output_directory = os.getenv('OUTPUT_DIR', './output')
        
        return config
