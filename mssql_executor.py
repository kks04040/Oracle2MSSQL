"""
Oracle to MSSQL DDL Converter
MSSQL DDL Executor - connects to MSSQL and executes converted DDL statements.
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Optional

from config import MSSQLConfig

logger = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    """Result of a DDL execution batch."""
    total_statements: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    errors: list = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if self.total_statements == 0:
            return 100.0
        return (self.succeeded / self.total_statements) * 100


class MSSQLExecutor:
    """Execute converted DDL statements against a MSSQL database."""

    def __init__(self, config: MSSQLConfig):
        """Initialize with MSSQL connection configuration.

        Args:
            config: MSSQLConfig with connection parameters.
        """
        self.config = config
        self._connection = None

    def connect(self) -> None:
        """Establish connection to MSSQL database."""
        import pymssql

        logger.info(
            "Connecting to MSSQL at %s:%d/%s as %s",
            self.config.host, self.config.port,
            self.config.database, self.config.username,
        )
        self._connection = pymssql.connect(
            server=self.config.host,
            port=str(self.config.port),
            user=self.config.username,
            password=self.config.password,
            database=self.config.database,
            login_timeout=self.config.login_timeout,
            timeout=self.config.query_timeout,
        )
        # Enable autocommit so each DDL statement is immediately committed
        conn = self._connection
        if hasattr(conn, 'autocommit'):
            conn.autocommit(True)
        logger.info("Connected to MSSQL successfully")

    def close(self) -> None:
        """Close the MSSQL connection."""
        if self._connection:
            try:
                self._connection.close()
            except Exception as e:
                logger.warning("Error closing MSSQL connection: %s", e)
            self._connection = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _split_statements(self, script: str) -> list[str]:
        """Split a SQL script into individual statements.

        Handles multi-line statements, comments, and GO batch separators.
        """
        statements = []
        current = []
        in_comment = False
        in_string = False
        string_char = None

        lines = script.split('\n')
        for line in lines:
            stripped = line.strip()

            # Skip pure comment lines (not inside a statement)
            if not current and stripped.startswith('--'):
                continue

            # Handle GO batch separator
            if stripped.upper() == 'GO':
                if current:
                    stmt = '\n'.join(current).strip()
                    if stmt and not stmt.startswith('--'):
                        statements.append(stmt)
                    current = []
                continue

            # Accumulate lines
            current.append(line)

            # Check if line ends with semicolon (statement terminator)
            if stripped.endswith(';'):
                stmt = '\n'.join(current).strip()
                if stmt and not stmt.startswith('--'):
                    statements.append(stmt)
                current = []

        # Handle remaining lines without semicolon
        if current:
            stmt = '\n'.join(current).strip()
            if stmt and not stmt.startswith('--'):
                statements.append(stmt)

        return statements

    def execute_script(self, script: str, dry_run: bool = False) -> ExecutionResult:
        """Execute a full DDL script against MSSQL.

        Args:
            script: Complete SQL script with one or more statements.
            dry_run: If True, parse and count statements without executing.

        Returns:
            ExecutionResult with success/failure counts and error details.
        """
        result = ExecutionResult()
        statements = self._split_statements(script)
        result.total_statements = len(statements)

        if dry_run:
            result.skipped = len(statements)
            logger.info("Dry run: %d statements parsed (not executed)", len(statements))
            return result

        if not self._connection:
            self.connect()

        import pymssql

        for i, stmt in enumerate(statements, 1):
            try:
                logger.debug("Executing statement %d/%d", i, len(statements))
                assert self._connection is not None
                cursor = self._connection.cursor()
                cursor.execute(stmt)
                cursor.close()
                result.succeeded += 1
                logger.info("  [%d/%d] OK: %s", i, len(statements), _statement_summary(stmt))
            except pymssql.OperationalError as e:
                # Connection-level errors (e.g. table already exists with IF NOT EXISTS)
                if 'already exists' in str(e).lower():
                    result.skipped += 1
                    logger.info("  [%d/%d] SKIP (already exists): %s", i, len(statements), _statement_summary(stmt))
                else:
                    result.failed += 1
                    result.errors.append({
                        'statement': _statement_summary(stmt),
                        'index': i,
                        'error': str(e),
                        'type': 'OperationalError',
                    })
                    logger.error("  [%d/%d] ERROR: %s", i, len(statements), e)
            except Exception as e:
                result.failed += 1
                result.errors.append({
                    'statement': _statement_summary(stmt),
                    'index': i,
                    'error': str(e),
                    'type': type(e).__name__,
                })
                logger.error("  [%d/%d] ERROR: %s", i, len(statements), e)

        return result

    def execute_file(self, filepath: str, dry_run: bool = False) -> ExecutionResult:
        """Execute DDL from a SQL file.

        Args:
            filepath: Path to the SQL file.
            dry_run: If True, parse without executing.

        Returns:
            ExecutionResult with details.
        """
        with open(filepath, 'r', encoding='utf-8') as f:
            script = f.read()
        return self.execute_script(script, dry_run=dry_run)


def _statement_summary(stmt: str, max_len: int = 80) -> str:
    """Return a short summary of a SQL statement for logging."""
    # Remove leading whitespace and comments
    cleaned = stmt.strip()
    cleaned = re.sub(r'^--.*?\n', '', cleaned, flags=re.MULTILINE).strip()
    # Take first line, truncate
    first_line = cleaned.split('\n')[0].strip()
    if len(first_line) > max_len:
        first_line = first_line[:max_len] + '...'
    return first_line
