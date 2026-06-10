#!/usr/bin/env python3
"""
Databricks equivalent of the Snowflake DEFOG_DAILY_UPDATE stored procedure.

Reads the DML statements from sf_task.sql, adapts them from Snowflake SQL
syntax to Databricks SQL syntax, and executes them against a Databricks
SQL warehouse.  A Delta lock table (defog.broker.daily_update_lock) prevents
duplicate runs on the same calendar day.

Usage
-----
First-time setup — creates catalog, schemas, and all DEFOG tables:

    python databricks_task.py --init

Daily refresh (skips if already run today):

    python databricks_task.py

Force a re-run on a day already processed (e.g. for testing):

    python databricks_task.py --force

Environment variables required:
    DATABRICKS_HOST        e.g. "adb-123456.7.azuredatabricks.net"
    DATABRICKS_HTTP_PATH   e.g. "/sql/1.0/warehouses/abc123"
    DATABRICKS_TOKEN       Personal-access token or service-principal secret
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Syntax adapters: Snowflake SQL → Databricks SQL
# ---------------------------------------------------------------------------

_INTERVAL_UNIT_MAP: dict[str, str] = {
    "second": "SECOND",
    "seconds": "SECOND",
    "minute": "MINUTE",
    "minutes": "MINUTE",
    "min": "MINUTE",
    "hour": "HOUR",
    "hours": "HOUR",
    "day": "DAY",
    "days": "DAY",
    "week": "WEEK",
    "weeks": "WEEK",
    "month": "MONTH",
    "months": "MONTH",
    "quarter": "QUARTER",
    "quarters": "QUARTER",
    "year": "YEAR",
    "years": "YEAR",
}


def _replace_interval(m: re.Match) -> str:
    num = m.group(1)
    unit = _INTERVAL_UNIT_MAP.get(m.group(2).lower(), m.group(2).upper())
    return f"INTERVAL {num} {unit}"


# Snowflake's TO_CHAR (as used in sf_task.sql) takes MySQL-style "%"
# format codes; Databricks' DATE_FORMAT expects Java SimpleDateFormat
# pattern letters.
_TO_CHAR_FORMAT_MAP: dict[str, str] = {
    "Y": "yyyy",
    "y": "yy",
    "m": "MM",
    "c": "M",
    "d": "dd",
    "e": "d",
    "H": "HH",
    "k": "H",
    "h": "hh",
    "I": "hh",
    "i": "mm",
    "s": "ss",
    "S": "ss",
    "p": "a",
    "M": "MMMM",
    "b": "MMM",
    "a": "EEE",
    "W": "EEEE",
    "j": "DDD",
    "f": "SSSSSS",
    "%": "%",
}


def _convert_date_format_string(fmt: str) -> str:
    """Convert a MySQL-style '%Y%m%d ...' format string to Databricks'
    Java-pattern equivalent (e.g. 'yyyyMMdd ...')."""
    return re.sub(
        r"%(.)", lambda m: _TO_CHAR_FORMAT_MAP.get(m.group(1), m.group(1)), fmt
    )


def _convert_to_char_calls(sql: str) -> str:
    """
    Replace each TO_CHAR(expr, '%fmt') call with DATE_FORMAT(expr, 'fmt')
    using Databricks-compatible pattern letters.
    """
    pattern = re.compile(r"TO_CHAR\s*\(", re.IGNORECASE)
    result: list[str] = []
    pos = 0
    while True:
        m = pattern.search(sql, pos)
        if not m:
            result.append(sql[pos:])
            break
        result.append(sql[pos : m.start()])

        # Find the matching closing paren, tracking nesting and strings.
        depth = 1
        j = m.end()
        in_string = False
        while j < len(sql) and depth > 0:
            ch = sql[j]
            if ch == "'" and not in_string:
                in_string = True
            elif ch == "'" and in_string:
                if j + 1 < len(sql) and sql[j + 1] == "'":
                    j += 2
                    continue
                in_string = False
            elif not in_string and ch == "(":
                depth += 1
            elif not in_string and ch == ")":
                depth -= 1
            j += 1

        call_args = sql[m.end() : j - 1]
        args = _split_top_level(call_args, ",")
        fmt_match = (
            re.match(r"^\s*'(.*)'\s*$", args[1], re.DOTALL) if len(args) == 2 else None
        )
        if fmt_match:
            new_fmt = _convert_date_format_string(fmt_match.group(1))
            result.append(f"DATE_FORMAT({args[0].strip()}, '{new_fmt}')")
        else:
            result.append(f"DATE_FORMAT({call_args})")
        pos = j

    return "".join(result)


def adapt_for_databricks(sql: str) -> str:
    """
    Convert Snowflake-specific SQL idioms to their Databricks equivalents:

    - 'literal'::timestamp  →  TIMESTAMP 'literal'
    - INTERVAL 'N unit'     →  INTERVAL N UNIT
    - EXTRACT(EPOCH FROM …) →  UNIX_TIMESTAMP(…)
    - TO_CHAR(…)            →  DATE_FORMAT(…)
    - DEFOG.SCHEMA.TABLE    →  defog.schema.table
    """
    # PostgreSQL-style timestamp cast
    sql = re.sub(r"'([^']+)'::timestamp", r"TIMESTAMP '\1'", sql)

    # INTERVAL 'N unit' → INTERVAL N UNIT
    sql = re.sub(
        r"\bINTERVAL\s+'(\d+)\s+(\w+)'",
        _replace_interval,
        sql,
        flags=re.IGNORECASE,
    )

    # EXTRACT(EPOCH FROM expr)  →  UNIX_TIMESTAMP(expr)
    # The closing ) of EXTRACT naturally becomes the closing ) of UNIX_TIMESTAMP.
    sql = re.sub(
        r"\bEXTRACT\s*\(\s*EPOCH\s+FROM\s*",
        "UNIX_TIMESTAMP(",
        sql,
        flags=re.IGNORECASE,
    )

    # TO_CHAR(expr, '%Y%m%d ...') → DATE_FORMAT(expr, 'yyyyMMdd ...')
    sql = _convert_to_char_calls(sql)

    # DEFOG.SCHEMA.TABLE → defog.schema.table
    sql = re.sub(
        r"\bDEFOG\.([A-Z_]+)\.([A-Z_]+)\b",
        lambda m: f"defog.{m.group(1).lower()}.{m.group(2).lower()}",
        sql,
    )

    # Normalize mixed date/timestamp literal types within INSERT VALUES
    if re.match(r"INSERT\s+INTO", sql, re.IGNORECASE):
        sql = _normalize_insert_value_types(sql)

    return sql


_DATE_LITERAL_RE = re.compile(r"^'(\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2})?)'$")
_DATE_TYPED_EXPR_RE = re.compile(
    r"\b(CURRENT_TIMESTAMP|CURRENT_DATE|DATE_TRUNC)\b", re.IGNORECASE
)


def _split_top_level(text: str, sep: str) -> list[str]:
    """
    Split text on a separator, ignoring occurrences inside parentheses or
    single-quoted string literals.
    """
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    in_string = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "'" and not in_string:
            in_string = True
            buf.append(ch)
        elif ch == "'" and in_string:
            if i + 1 < len(text) and text[i + 1] == "'":
                buf.append("''")
                i += 2
                continue
            in_string = False
            buf.append(ch)
        elif not in_string and ch == "(":
            depth += 1
            buf.append(ch)
        elif not in_string and ch == ")":
            depth -= 1
            buf.append(ch)
        elif not in_string and depth == 0 and text[i : i + len(sep)] == sep:
            parts.append("".join(buf))
            buf = []
            i += len(sep)
            continue
        else:
            buf.append(ch)
        i += 1
    parts.append("".join(buf))
    return parts


def _extract_top_level_groups(text: str) -> list[str]:
    """
    Return the contents (without enclosing parens) of each top-level
    parenthesized group in text, e.g. "(1, 2), (3, 4)" → ["1, 2", "3, 4"].
    """
    groups: list[str] = []
    buf: list[str] = []
    depth = 0
    in_string = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "'" and not in_string:
            in_string = True
        elif ch == "'" and in_string:
            if i + 1 < len(text) and text[i + 1] == "'":
                buf.append("''")
                i += 2
                continue
            in_string = False
        elif not in_string and ch == "(":
            depth += 1
            if depth == 1:
                i += 1
                continue
        elif not in_string and ch == ")":
            depth -= 1
            if depth == 0:
                groups.append("".join(buf))
                buf = []
                i += 1
                continue
        if depth >= 1:
            buf.append(ch)
        i += 1
    return groups


def _normalize_insert_value_types(sql: str) -> str:
    """
    For INSERT ... VALUES (...), (...), ... statements, find columns where
    at least one row supplies a TIMESTAMP/DATE-typed expression (e.g.
    CURRENT_TIMESTAMP - INTERVAL ...) and convert plain ISO date/timestamp
    string literals in that same column position to TIMESTAMP '...'
    literals. This avoids Databricks' INVALID_INLINE_TABLE error caused by
    mixing STRING and TIMESTAMP/DATE values in the same inline-table column.
    """
    match = re.search(r"\bVALUES\b", sql, re.IGNORECASE)
    if not match:
        return sql

    header = sql[: match.end()]
    body = sql[match.end() :].strip()

    rows: list[list[str]] = [
        [c.strip() for c in _split_top_level(group, ",")]
        for group in _extract_top_level_groups(body)
    ]

    if not rows or any(len(r) != len(rows[0]) for r in rows):
        return sql

    num_cols = len(rows[0])
    for col in range(num_cols):
        if any(_DATE_TYPED_EXPR_RE.search(row[col]) for row in rows):
            for row in rows:
                lit_match = _DATE_LITERAL_RE.match(row[col])
                if lit_match:
                    row[col] = f"TIMESTAMP '{lit_match.group(1)}'"

    rendered_rows = ",\n  ".join("(" + ", ".join(row) + ")" for row in rows)
    return f"{header}\n  {rendered_rows}"


# ---------------------------------------------------------------------------
# SQL extraction from sf_task.sql
# ---------------------------------------------------------------------------


def _split_on_semicolons(text: str) -> list[str]:
    """
    Split SQL text on top-level semicolons (ignores semicolons inside
    single-quoted string literals).
    """
    stmts: list[str] = []
    buf: list[str] = []
    in_string = False
    i = 0
    while i < len(text):
        ch = text[i]
        if not in_string and ch == "-" and text[i + 1 : i + 2] == "-":
            # Line comment: copy through end of line untouched so that
            # apostrophes (e.g. "today's") don't desync string tracking.
            line_end = text.find("\n", i)
            if line_end == -1:
                line_end = len(text)
            buf.append(text[i:line_end])
            i = line_end
            continue
        if ch == "'" and not in_string:
            in_string = True
            buf.append(ch)
        elif ch == "'" and in_string:
            # Handle escaped quote ''
            if i + 1 < len(text) and text[i + 1] == "'":
                buf.append("''")
                i += 2
                continue
            in_string = False
            buf.append(ch)
        elif ch == ";" and not in_string:
            stmt = "".join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


def extract_dml_statements(sf_task_path: Path) -> list[str]:
    """
    Parse sf_task.sql and return the DELETE + INSERT statements that live
    inside the stored-procedure body, adapted for Databricks SQL syntax.
    """
    content = sf_task_path.read_text()

    # The DML lives between the first BEGIN and the "Reset time session" comment.
    begin = re.search(r"\bBEGIN\b", content)
    end = re.search(r"--\s*Reset time session back", content)
    if not begin or not end:
        raise RuntimeError(
            "Could not locate procedure body in sf_task.sql. "
            "Expected a BEGIN marker and a '-- Reset time session back' comment."
        )

    body = content[begin.end() : end.start()]
    stmts = _split_on_semicolons(body)

    dml: list[str] = []
    for stmt in stmts:
        # Strip leading blank lines and comment-only lines (e.g. "--" separator
        # banners or explanatory comments) before checking the statement type,
        # so DELETE/INSERT statements preceded by such lines aren't dropped.
        lines = stmt.lstrip().splitlines()
        i = 0
        while i < len(lines) and (
            lines[i].strip() == "" or lines[i].strip().startswith("--")
        ):
            i += 1
        stripped = "\n".join(lines[i:]).lstrip()
        if re.match(r"(DELETE\s+FROM|INSERT\s+INTO)", stripped, re.IGNORECASE):
            dml.append(adapt_for_databricks(stripped))

    return dml


# ---------------------------------------------------------------------------
# DDL setup: create catalog, schemas, and tables (run once via --init,
# or implicitly on first connection via run_with_cursor)
# ---------------------------------------------------------------------------

# Schemas present in init_defog_sf.sql that sf_task.sql refreshes
_DEFOG_SCHEMAS = {"broker", "ewallet", "dealership", "dermtreatment"}


def _adapt_create_table(sql: str, schema: str | None) -> str:
    """
    Adapt a Snowflake CREATE TABLE statement to Databricks Delta syntax:
    - Add IF NOT EXISTS and defog.<schema>. prefix
    - Remove PRIMARY KEY (both inline and standalone constraint)
    - Remove AUTOINCREMENT
    - Add USING DELTA
    """
    table_match = re.search(r"CREATE\s+TABLE\s+(\w+)\s*\(", sql, re.IGNORECASE)
    if not table_match:
        return sql
    table_name = table_match.group(1).lower()

    sql = re.sub(
        r"CREATE\s+TABLE\s+\w+\s*\(",
        f"CREATE TABLE IF NOT EXISTS defog.{schema}.{table_name} (",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )

    # Process line by line so that standalone constraint detection runs
    # BEFORE any inline keyword removal (which would otherwise obscure them).
    lines = sql.split("\n")
    cleaned: list[str] = []
    for line in lines:
        # Standalone constraint lines — drop entirely and clean trailing comma
        if re.match(
            r"\s*(PRIMARY\s+KEY\s*\(|CONSTRAINT\b.*\bUNIQUE)",
            line,
            re.IGNORECASE,
        ):
            for j in range(len(cleaned) - 1, -1, -1):
                if cleaned[j].strip():
                    cleaned[j] = re.sub(r",\s*$", "", cleaned[j])
                    break
            continue

        # Inline constraint and type transformations per column line
        line = re.sub(r"\s+AUTOINCREMENT\b", "", line, flags=re.IGNORECASE)
        line = re.sub(r"\s+PRIMARY\s+KEY\b", "", line, flags=re.IGNORECASE)
        line = re.sub(r"\s+UNIQUE\b", "", line, flags=re.IGNORECASE)
        line = re.sub(r"\s+REFERENCES\s+\w+\s*\([^)]*\)", "", line, flags=re.IGNORECASE)
        # Types not supported in Databricks — map to nearest equivalent
        line = re.sub(r"\bTEXT\b", "STRING", line, flags=re.IGNORECASE)
        line = re.sub(r"\bNUMBER\b", "DECIMAL", line, flags=re.IGNORECASE)
        line = re.sub(r"\bBLOB\b", "BINARY", line, flags=re.IGNORECASE)
        # Databricks uses backticks for quoted identifiers, not double quotes
        line = re.sub(r'"([^"]*)"', r"`\1`", line)
        cleaned.append(line)

    # Strip trailing semicolon; the closing ) is already present, just append USING DELTA.
    # allowColumnDefaults is required for any column that has a DEFAULT value.
    sql = "\n".join(cleaned).rstrip().rstrip(";").rstrip()
    return (
        sql
        + " USING DELTA TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')"
    )


def _extract_refreshed_tables(sf_task_path: Path) -> set[str]:
    """Return the set of lowercase table names that sf_task.sql refreshes."""
    content = sf_task_path.read_text()
    return {
        m.group(1).lower()
        for m in re.finditer(r"DELETE\s+FROM\s+\w+\.\w+\.(\w+)", content, re.IGNORECASE)
    }


def extract_and_adapt_ddl(init_defog_sf_path: Path) -> list[str]:
    """
    Parse init_defog_sf.sql and return CREATE TABLE statements adapted for
    Databricks, limited to the tables that sf_task.sql actually refreshes.
    Skips edge-case test tables (reserved-word names, special characters, etc.)
    that are in init_defog_sf.sql but not used by sf_task.sql.
    """
    sf_task_path = init_defog_sf_path.with_name("sf_task.sql")
    refreshed_tables = (
        _extract_refreshed_tables(sf_task_path) if sf_task_path.exists() else set()
    )

    content = init_defog_sf_path.read_text()
    stmts = _split_on_semicolons(content)

    current_schema: str | None = None
    result: list[str] = []
    for stmt in stmts:
        stripped = stmt.strip()
        # Use re.search (not re.match) because statements may start with
        # comment blocks before the actual SQL keyword.
        schema_match = re.search(r"CREATE\s+SCHEMA\s+(\w+)", stripped, re.IGNORECASE)
        if schema_match:
            current_schema = schema_match.group(1).lower()
            # A missing semicolon after CREATE SCHEMA can leave a CREATE
            # TABLE statement bundled into the same chunk; keep processing
            # whatever follows the schema name instead of dropping it.
            stripped = stripped[schema_match.end() :].strip()
            if not stripped:
                continue
        table_match = current_schema in _DEFOG_SCHEMAS and re.search(
            r"CREATE\s+TABLE\s+(\w+)", stripped, re.IGNORECASE
        )
        if table_match:
            table_name = table_match.group(1).lower()
            if not refreshed_tables or table_name in refreshed_tables:
                result.append(_adapt_create_table(stripped, current_schema))

    return result


def setup_tables(cursor, init_defog_sf_path: Path | None = None) -> None:
    """
    Create the defog catalog, schemas, and all tables required by sf_task.sql.
    Safe to call multiple times — uses CREATE TABLE IF NOT EXISTS throughout.
    """
    if init_defog_sf_path is None:
        init_defog_sf_path = Path(__file__).with_name("init_defog_sf.sql")
    if not init_defog_sf_path.exists():
        raise FileNotFoundError(f"init_defog_sf.sql not found at {init_defog_sf_path}")

    print("Setting up defog catalog and schemas …")
    cursor.execute("CREATE CATALOG IF NOT EXISTS defog")
    for schema in _DEFOG_SCHEMAS:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS defog.{schema}")

    print("Creating tables from init_defog_sf.sql …")
    for stmt in extract_and_adapt_ddl(init_defog_sf_path):
        cursor.execute(stmt)

    print("Table setup complete.")


# ---------------------------------------------------------------------------
# Lock helpers
# ---------------------------------------------------------------------------

_LOCK_TABLE = "defog.broker.daily_update_lock"

_LOCK_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {_LOCK_TABLE} (
    lock_date DATE
) USING DELTA
"""


def _acquire_lock(cursor) -> bool:
    """
    Attempt to acquire today's lock row.  Returns True if the lock was
    successfully acquired (first run today), False if already locked.

    Uses a MERGE so the check-and-insert is atomic on a Delta table.
    After the MERGE we verify the outcome with a SELECT.
    """
    cursor.execute(
        f"""
        MERGE INTO {_LOCK_TABLE} AS target
        USING (SELECT CURRENT_DATE() AS lock_date) AS source
        ON target.lock_date = source.lock_date
        WHEN NOT MATCHED THEN INSERT (lock_date) VALUES (source.lock_date)
        """
    )
    # Verify the row now exists (handles both "just inserted" and "already existed")
    cursor.execute(
        f"SELECT COUNT(*) FROM {_LOCK_TABLE} WHERE lock_date = CURRENT_DATE()"
    )
    return (
        cursor.fetchone()[0] == 1
    )  # True = exactly one row → we own it if we just ran MERGE


def _release_lock(cursor) -> None:
    cursor.execute(f"DELETE FROM {_LOCK_TABLE} WHERE lock_date = CURRENT_DATE()")


def _cleanup_old_locks(cursor) -> None:
    cursor.execute(f"DELETE FROM {_LOCK_TABLE} WHERE lock_date < CURRENT_DATE()")


# ---------------------------------------------------------------------------
# Core refresh logic (cursor-based, reusable from test fixtures)
# ---------------------------------------------------------------------------


def run_with_cursor(
    cursor,
    sf_task_path: Path | None = None,
    force: bool = False,
) -> bool:
    """
    Run the DEFOG daily refresh using an already-open cursor.

    Mirrors the inline refresh check that ``sf_conn_db_context`` does for
    Snowflake: the caller establishes the connection, passes the cursor here,
    and we handle locking and DML.

    Args:
        cursor: An open ``databricks.sql`` cursor.
        sf_task_path: Path to ``sf_task.sql``.  Defaults to the copy next to
            this file.
        force: If True, drop today's lock first so the refresh re-runs.

    Returns:
        True if the refresh ran, False if data was already fresh today.
    """
    if sf_task_path is None:
        sf_task_path = Path(__file__).with_name("sf_task.sql")

    setup_tables(
        cursor, sf_task_path.with_name("init_defog_sf.sql") if sf_task_path else None
    )
    cursor.execute(_LOCK_TABLE_DDL)

    if force:
        _release_lock(cursor)

    cursor.execute(
        f"SELECT COUNT(*) FROM {_LOCK_TABLE} WHERE lock_date = CURRENT_DATE()"
    )
    if cursor.fetchone()[0] > 0:
        print("Daily update already completed today — skipping.")
        return False

    cursor.execute(f"INSERT INTO {_LOCK_TABLE} VALUES (CURRENT_DATE())")
    _cleanup_old_locks(cursor)

    dml_stmts = extract_dml_statements(sf_task_path)
    print(f"Running DEFOG refresh ({len(dml_stmts)} statements) …")
    try:
        for i, stmt in enumerate(dml_stmts, 1):
            preview = stmt.splitlines()[0][:80]
            print(f"  [{i}/{len(dml_stmts)}] {preview}")
            cursor.execute(stmt)
    except Exception:
        print("Error — releasing lock so today can be retried.")
        _release_lock(cursor)
        raise

    print("Daily update completed successfully.")
    return True


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main(force: bool = False) -> None:
    host = os.environ.get("DATABRICKS_HOST")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    token = os.environ.get("DATABRICKS_TOKEN")

    if not all([host, http_path, token]):
        sys.exit(
            "Missing required environment variables: "
            "DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN"
        )

    init_only = "--init" in sys.argv

    from databricks import sql as dbsql

    with dbsql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    ) as conn:
        with conn.cursor() as cursor:
            if init_only:
                setup_tables(cursor)
            else:
                run_with_cursor(cursor, force=force)


if __name__ == "__main__":
    main(force="--force" in sys.argv)
