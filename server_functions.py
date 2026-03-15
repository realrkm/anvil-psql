import os
import shlex
import threading
import psycopg2
import psycopg2.pool
from psycopg2 import sql
from psycopg2.extras import execute_values
import anvil.server

# ============================================================
# CONNECTION POOL  (created once at module load)
# ============================================================
# Reads data_path from env var — falls back to ".anvil-data"
# Increase max_connections if your app has high concurrency.

_pool: psycopg2.pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()

# Module-level table name cache to avoid a DB round-trip on every
# validate_table() call.  Invalidated whenever DDL changes a table.
_table_cache: set[str] | None = None
_table_cache_lock = threading.Lock()


def _build_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Read connection params from .anvil-data and build a connection pool."""
    data_path = os.environ.get("ANVIL_DATA_PATH", ".anvil-data")

    opts_path = os.path.join(data_path, "db", "postmaster.opts")
    try:
        with open(opts_path) as f:
            opts = f.read()
    except OSError:
        raise RuntimeError(
            f"Cannot open {opts_path}. "
            "Is ANVIL_DATA_PATH set correctly and the app server running?"
        )

    port = None
    last = None
    for opt in shlex.split(opts):
        if last == "-p":
            port = int(opt)
            break
        last = opt
    if port is None:
        raise RuntimeError(f"Could not determine PostgreSQL port from {opts_path}.")

    pw_path = os.path.join(data_path, "postgres.password")
    try:
        with open(pw_path) as f:
            password = f.read().strip()
    except OSError:
        raise RuntimeError(f"Cannot open {pw_path}.")

    return psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=int(os.environ.get("ANVIL_DB_POOL_SIZE", "10")),
        host="localhost",
        port=port,
        user="postgres",
        password=password,
        dbname="postgres",
        connect_timeout=10,
        options=(
            "-c search_path=app_tables "
            "-c statement_timeout=10000 "   # kill queries > 10 s
            "-c lock_timeout=5000"          # don't wait forever for locks
        )
    )


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Return the module-level pool, initialising it on first call."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:           # double-checked locking
                _pool = _build_pool()
    return _pool


class _PooledConn:
    """Context manager: borrows a connection from the pool and always returns it."""
    def __init__(self):
        self._conn = None

    def __enter__(self) -> psycopg2.extensions.connection:
        self._conn = _get_pool().getconn()
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn is not None:
            if exc_type is not None:
                self._conn.rollback()
            _get_pool().putconn(self._conn)
        return False    # never suppress exceptions


# ============================================================
# TABLE-NAME CACHE
# ============================================================

def _refresh_table_cache(cur) -> set[str]:
    """Query information_schema and repopulate the module-level cache."""
    global _table_cache
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'app_tables'
    """)
    tables = {row[0] for row in cur.fetchall()}
    with _table_cache_lock:
        _table_cache = tables
    return tables


def _get_table_cache(cur) -> set[str]:
    """Return cached table names, populating the cache if empty."""
    global _table_cache
    if _table_cache is None:
        return _refresh_table_cache(cur)
    with _table_cache_lock:
        return set(_table_cache)


def _invalidate_table_cache():
    global _table_cache
    with _table_cache_lock:
        _table_cache = None


# ============================================================
# INTERNAL VALIDATORS  (reuse an already-open cursor)
# ============================================================

def _validate_table(cur, table_name: str):
    if table_name not in _get_table_cache(cur):
        raise ValueError(f"Table '{table_name}' not found.")


def _validate_columns(cur, table_name: str, column_names: list[str]):
    """Confirm every column in column_names exists on table_name."""
    if not column_names:
        return
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'app_tables' AND table_name = %s
    """, (table_name,))
    valid = {row[0] for row in cur.fetchall()}
    invalid = [c for c in column_names if c not in valid]
    if invalid:
        raise ValueError(f"Unknown column(s) on '{table_name}': {invalid}")


def _safe_table_name(table_name: str):
    """Reject table names that contain anything other than letters/digits/underscores."""
    if not table_name.replace("_", "").isalnum():
        raise ValueError(
            f"Invalid table name '{table_name}'. "
            "Only letters, digits, and underscores are allowed."
        )


# ============================================================
# READ
# ============================================================

@anvil.server.callable
def get_tables() -> list[str]:
    """Return a sorted list of all table names in the app_tables schema."""
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            return sorted(_refresh_table_cache(cur))


@anvil.server.callable
def get_columns(table_name: str) -> list[dict]:
    """Return column names and data types for *table_name*."""
    _safe_table_name(table_name)
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            _validate_table(cur, table_name)
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'app_tables' AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            return [{"column": r[0], "type": r[1]} for r in cur.fetchall()]


@anvil.server.callable
def get_row_count(table_name: str, filters: dict | None = None) -> int:
    """Return the number of rows in *table_name*, optionally filtered."""
    _safe_table_name(table_name)
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            _validate_table(cur, table_name)
            params = []
            where_clause = sql.SQL("")
            if filters:
                _validate_columns(cur, table_name, list(filters.keys()))
                conditions = sql.SQL(" AND ").join(
                    sql.SQL("{} = %s").format(sql.Identifier(c))
                    for c in filters.keys()
                )
                where_clause = sql.SQL(" WHERE {}").format(conditions)
                params = list(filters.values())
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {t}{w}").format(
                    t=sql.Identifier(table_name), w=where_clause
                ),
                params
            )
            return cur.fetchone()[0]


@anvil.server.callable
def query_table(
    table_name: str,
    filters: dict | None = None,
    order_by: str | None = None,
    order_dir: str = "ASC",
    page: int = 1,
    page_size: int = 100,
) -> dict:
    """
    Query *table_name* with optional filtering, sorting, and pagination.

    Returns:
        {
            'rows':        list[dict],
            'page':        int,
            'page_size':   int,
            'total':       int,
            'total_pages': int,
        }
    """
    _safe_table_name(table_name)

    order_dir = order_dir.upper()
    if order_dir not in ("ASC", "DESC"):
        raise ValueError("order_dir must be 'ASC' or 'DESC'.")
    if page < 1:
        raise ValueError("page must be >= 1.")
    if not (1 <= page_size <= 1000):
        raise ValueError("page_size must be between 1 and 1000.")

    offset = (page - 1) * page_size

    with _PooledConn() as conn:
        with conn.cursor() as cur:
            _validate_table(cur, table_name)

            check_cols = list(filters.keys()) if filters else []
            if order_by:
                check_cols.append(order_by)
            if check_cols:
                _validate_columns(cur, table_name, check_cols)

            # WHERE
            params: list = []
            where_clause = sql.SQL("")
            if filters:
                conditions = sql.SQL(" AND ").join(
                    sql.SQL("{} = %s").format(sql.Identifier(c))
                    for c in filters.keys()
                )
                where_clause = sql.SQL(" WHERE {}").format(conditions)
                params = list(filters.values())

            # COUNT (reuse same cursor / connection)
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {t}{w}").format(
                    t=sql.Identifier(table_name), w=where_clause
                ),
                params,
            )
            total: int = cur.fetchone()[0]
            total_pages = max(1, -(-total // page_size))  # ceiling division

            # ORDER BY
            order_clause = sql.SQL("")
            if order_by:
                order_clause = sql.SQL(" ORDER BY {} {}").format(
                    sql.Identifier(order_by), sql.SQL(order_dir)
                )

            # SELECT
            cur.execute(
                sql.SQL("SELECT * FROM {t}{w}{o} LIMIT %s OFFSET %s").format(
                    t=sql.Identifier(table_name),
                    w=where_clause,
                    o=order_clause,
                ),
                params + [page_size, offset],
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

            return {
                "rows": rows,
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": total_pages,
            }


@anvil.server.callable
def get_row_by_id(table_name: str, row_id: int) -> dict | None:
    """Return the row with the given *row_id*, or ``None`` if not found."""
    _safe_table_name(table_name)
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            _validate_table(cur, table_name)
            cur.execute(
                sql.SQL("SELECT * FROM {} WHERE id = %s").format(
                    sql.Identifier(table_name)
                ),
                (row_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))


# run_query is intentionally NOT decorated with @anvil.server.callable.
# Expose it only from a dedicated admin-only wrapper that enforces auth:
#
#   @anvil.server.callable(require_user=True)
#   def admin_run_query(sql_string, params=None):
#       if not anvil.users.get_user()['is_admin']:
#           raise Exception("Admin only.")
#       return _run_query(sql_string, params)

def _run_query(sql_string: str, params=None) -> list[dict]:
    """
    Execute a read-only SQL query.
    Uses a true READ ONLY transaction — not just a string prefix check.
    """
    with _PooledConn() as conn:
        # Force the entire transaction to be read-only at the DB level.
        conn.set_session(readonly=True, autocommit=False)
        try:
            with conn.cursor() as cur:
                cur.execute(sql_string, params or [])
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            # Always restore the connection to read-write before returning
            # it to the pool.
            conn.rollback()
            conn.set_session(readonly=False, autocommit=False)


# ============================================================
# INSERT
# ============================================================

@anvil.server.callable
def insert_row(table_name: str, data: dict) -> dict:
    """Insert a single row and return it (including its generated ``id``)."""
    _safe_table_name(table_name)
    if not data:
        raise ValueError("data must not be empty.")
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            _validate_table(cur, table_name)
            _validate_columns(cur, table_name, list(data.keys()))
            cols = sql.SQL(", ").join(map(sql.Identifier, data.keys()))
            placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(data))
            cur.execute(
                sql.SQL(
                    "INSERT INTO {t} ({c}) VALUES ({p}) RETURNING *"
                ).format(
                    t=sql.Identifier(table_name), c=cols, p=placeholders
                ),
                list(data.values()),
            )
            conn.commit()
            row = cur.fetchone()
            col_names = [d[0] for d in cur.description]
            return dict(zip(col_names, row))


@anvil.server.callable
def insert_many_rows(table_name: str, rows: list[dict]) -> int:
    """
    Insert *rows* atomically using a single ``execute_values`` call.
    All rows succeed or all are rolled back.
    Returns the number of rows inserted.
    """
    if not rows:
        return 0
    _safe_table_name(table_name)
    # Ensure every row has the same keys (in the same order)
    keys = list(rows[0].keys())
    if any(list(r.keys()) != keys for r in rows[1:]):
        raise ValueError("All rows must have identical keys in the same order.")
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            _validate_table(cur, table_name)
            _validate_columns(cur, table_name, keys)
            cols = sql.SQL(", ").join(map(sql.Identifier, keys))
            base_query = sql.SQL(
                "INSERT INTO {t} ({c}) VALUES %s"
            ).format(t=sql.Identifier(table_name), c=cols)
            execute_values(
                cur,
                base_query.as_string(conn),
                [list(r.values()) for r in rows],
            )
            conn.commit()
            return cur.rowcount


# ============================================================
# UPDATE
# ============================================================

@anvil.server.callable
def update_row(table_name: str, row_id: int, data: dict) -> dict:
    """Update the row identified by *row_id* and return the updated row."""
    _safe_table_name(table_name)
    if not data:
        raise ValueError("data must not be empty.")
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            _validate_table(cur, table_name)
            _validate_columns(cur, table_name, list(data.keys()))
            assignments = sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(c)) for c in data.keys()
            )
            cur.execute(
                sql.SQL(
                    "UPDATE {t} SET {a} WHERE id = %s RETURNING *"
                ).format(t=sql.Identifier(table_name), a=assignments),
                list(data.values()) + [row_id],
            )
            conn.commit()
            row = cur.fetchone()
            if row is None:
                raise ValueError(
                    f"Row with id={row_id} not found in '{table_name}'."
                )
            col_names = [d[0] for d in cur.description]
            return dict(zip(col_names, row))


@anvil.server.callable
def update_rows_where(
    table_name: str, filters: dict, data: dict
) -> int:
    """
    Update all rows matching *filters* with *data*.
    Returns the number of rows updated.
    """
    _safe_table_name(table_name)
    if not filters:
        raise ValueError("filters must not be empty.")
    if not data:
        raise ValueError("data must not be empty.")
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            _validate_table(cur, table_name)
            _validate_columns(cur, table_name, list(data.keys()) + list(filters.keys()))
            assignments = sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(c)) for c in data.keys()
            )
            conditions = sql.SQL(" AND ").join(
                sql.SQL("{} = %s").format(sql.Identifier(c)) for c in filters.keys()
            )
            cur.execute(
                sql.SQL("UPDATE {t} SET {a} WHERE {w}").format(
                    t=sql.Identifier(table_name), a=assignments, w=conditions
                ),
                list(data.values()) + list(filters.values()),
            )
            conn.commit()
            return cur.rowcount


# ============================================================
# DELETE
# ============================================================

@anvil.server.callable
def delete_row(table_name: str, row_id: int) -> bool:
    """Delete the row with *row_id*. Returns ``True`` if a row was deleted."""
    _safe_table_name(table_name)
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            _validate_table(cur, table_name)
            cur.execute(
                sql.SQL("DELETE FROM {} WHERE id = %s").format(
                    sql.Identifier(table_name)
                ),
                (row_id,),
            )
            conn.commit()
            return cur.rowcount > 0


@anvil.server.callable
def delete_rows_where(table_name: str, filters: dict) -> int:
    """
    Delete all rows matching *filters*.
    Returns the number of rows deleted.
    """
    _safe_table_name(table_name)
    if not filters:
        raise ValueError(
            "filters must not be empty. Use delete_all_rows() to truncate a table."
        )
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            _validate_table(cur, table_name)
            _validate_columns(cur, table_name, list(filters.keys()))
            conditions = sql.SQL(" AND ").join(
                sql.SQL("{} = %s").format(sql.Identifier(c)) for c in filters.keys()
            )
            cur.execute(
                sql.SQL("DELETE FROM {t} WHERE {w}").format(
                    t=sql.Identifier(table_name), w=conditions
                ),
                list(filters.values()),
            )
            conn.commit()
            return cur.rowcount


@anvil.server.callable
def delete_all_rows(table_name: str) -> bool:
    """TRUNCATE *table_name*. All rows are removed and the change is irreversible."""
    _safe_table_name(table_name)
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            _validate_table(cur, table_name)
            cur.execute(
                sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table_name))
            )
            conn.commit()
            return True


# ============================================================
# TABLE MANAGEMENT
# ============================================================

@anvil.server.callable
def create_table(table_name: str, columns: list[dict]) -> bool:
    """
    Create a new table in the app_tables schema.

    *columns* is a list of dicts with keys:
        name        (str, required)
        type        (str, required)  — e.g. 'TEXT', 'INTEGER', 'BOOLEAN'
        constraints (str, optional) — e.g. 'NOT NULL', 'DEFAULT 0'

    An ``id SERIAL PRIMARY KEY`` column is always prepended automatically.
    """
    _safe_table_name(table_name)
    col_defs = [sql.SQL("id SERIAL PRIMARY KEY")]
    for col in columns:
        _safe_table_name(col["name"])           # reuse same name-safety check
        col_defs.append(
            sql.SQL("{} {} {}").format(
                sql.Identifier(col["name"]),
                sql.SQL(col["type"]),
                sql.SQL(col.get("constraints", "")),
            )
        )
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE TABLE IF NOT EXISTS {t} ({c})").format(
                    t=sql.Identifier(table_name),
                    c=sql.SQL(", ").join(col_defs),
                )
            )
            conn.commit()
            _invalidate_table_cache()           # force cache refresh next call
            return True


@anvil.server.callable
def drop_table(table_name: str) -> bool:
    """Drop *table_name* entirely. This is irreversible."""
    _safe_table_name(table_name)
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            _validate_table(cur, table_name)
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}").format(
                    sql.Identifier(table_name)
                )
            )
            conn.commit()
            _invalidate_table_cache()
            return True


@anvil.server.callable
def add_column(
    table_name: str,
    column_name: str,
    column_type: str,
    constraints: str = "",
) -> bool:
    """Add *column_name* of *column_type* to *table_name*."""
    _safe_table_name(table_name)
    _safe_table_name(column_name)
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            _validate_table(cur, table_name)
            cur.execute(
                sql.SQL("ALTER TABLE {t} ADD COLUMN {c} {ty} {co}").format(
                    t=sql.Identifier(table_name),
                    c=sql.Identifier(column_name),
                    ty=sql.SQL(column_type),
                    co=sql.SQL(constraints),
                )
            )
            conn.commit()
            return True


@anvil.server.callable
def drop_column(table_name: str, column_name: str) -> bool:
    """Remove *column_name* from *table_name*. This is irreversible."""
    _safe_table_name(table_name)
    _safe_table_name(column_name)
    with _PooledConn() as conn:
        with conn.cursor() as cur:
            _validate_table(cur, table_name)
            _validate_columns(cur, table_name, [column_name])
            cur.execute(
                sql.SQL("ALTER TABLE {t} DROP COLUMN {c}").format(
                    t=sql.Identifier(table_name),
                    c=sql.Identifier(column_name),
                )
            )
            conn.commit()
            return True
