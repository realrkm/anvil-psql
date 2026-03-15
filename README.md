# anvil-psql

> Full SQL access to your Anvil App Server's PostgreSQL database — with CRUD, table management, pagination, sorting, and atomic transactions.

This module exposes a set of `@anvil.server.callable` functions that let your Anvil client forms query and manage the PostgreSQL database managed by the [Anvil App Server](https://github.com/anvil-works/anvil-runtime) when running locally. It uses a **connection pool**, a **table name cache**, **DB-level read-only enforcement**, and **automatic rollback on failure**.

---

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Security Notes](#security-notes)
- [Function Reference](#function-reference)
- [Client-Side Examples](#client-side-examples)
  - [READ](#read)
  - [INSERT](#insert)
  - [UPDATE](#update)
  - [DELETE](#delete)
  - [Table Management](#table-management)
  - [Export and Import](#export-and-import)
- [Error Handling](#error-handling)
- [Complete Working Form Example](#complete-working-form-example)
- [Complete Export / Import Form Example](#complete-export--import-form-example)

---

## Requirements

- Python 3.10+
- `psycopg2-binary`
- Anvil App Server running locally

---

## Installation

**1. Install the dependency:**

```bash
pip install psycopg2-binary
```

**2. Copy `server_functions.py` into your app's Server Module** in the Anvil editor (or place it alongside your app entry point).

**3. Start your app server as normal:**

```bash
anvil-app-server --app MyApp
```

The module auto-discovers the PostgreSQL port and credentials from `.anvil-data/` — no manual configuration needed.

---

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `ANVIL_DATA_PATH` | `.anvil-data` | Path to the Anvil App Server data directory |
| `ANVIL_DB_POOL_SIZE` | `10` | Maximum number of pooled database connections |

```bash
# Example overrides before starting the app server
export ANVIL_DATA_PATH="/custom/path/to/.anvil-data"
export ANVIL_DB_POOL_SIZE="20"
anvil-app-server --app MyApp
```

---

## Security Notes

| Area | What this module does |
|---|---|
| **SQL injection** | All table names, column names, and values are passed through `psycopg2.sql.Identifier` or parameterised `%s` placeholders — never interpolated as raw strings |
| **Table/column name validation** | Every table and column name is checked against a whitelist of names that exist in the database before any query runs |
| **Read-only enforcement** | `_run_query` uses `conn.set_session(readonly=True)` — enforced at the PostgreSQL level, not just a string check |
| **`_run_query` not exposed** | The raw query helper is a private `_` function. Wrap it yourself with `require_user=True` and a role check before exposing it to clients (see [Error Handling](#error-handling)) |
| **Destructive operations** | `drop_table`, `drop_column`, and `delete_all_rows` should be guarded with admin role checks in your own callable wrappers |

---

## Function Reference

### Read

| Function | Description | Returns |
|---|---|---|
| `get_tables()` | List all table names in `app_tables` schema | `list[str]` |
| `get_columns(table_name)` | List columns and their data types | `list[dict]` |
| `get_row_count(table_name, filters?)` | Count rows, optionally filtered | `int` |
| `query_table(table_name, filters?, order_by?, order_dir?, page?, page_size?)` | Query with filter, sort, and pagination | `dict` (see below) |
| `get_row_by_id(table_name, row_id)` | Fetch a single row by primary key | `dict \| None` |

`query_table` always returns a pagination envelope:

```python
{
    "rows":        list[dict],  # the data
    "page":        int,
    "page_size":   int,
    "total":       int,         # total matching rows (ignoring pagination)
    "total_pages": int,
}
```

### Insert

| Function | Description | Returns |
|---|---|---|
| `insert_row(table_name, data)` | Insert one row | `dict` (the inserted row including its `id`) |
| `insert_many_rows(table_name, rows)` | Bulk insert — atomic, all-or-nothing | `int` (rows inserted) |

### Update

| Function | Description | Returns |
|---|---|---|
| `update_row(table_name, row_id, data)` | Update a single row by `id` | `dict` (the updated row) |
| `update_rows_where(table_name, filters, data)` | Update all rows matching `filters` | `int` (rows updated) |

### Delete

| Function | Description | Returns |
|---|---|---|
| `delete_row(table_name, row_id)` | Delete one row by `id` | `bool` |
| `delete_rows_where(table_name, filters)` | Delete all rows matching `filters` | `int` (rows deleted) |
| `delete_all_rows(table_name)` | Truncate the table — **irreversible** | `bool` |

### Table Management

| Function | Description | Returns |
|---|---|---|
| `create_table(table_name, columns)` | Create a new table (auto-adds `id SERIAL PRIMARY KEY`) | `bool` |
| `drop_table(table_name)` | Drop a table entirely — **irreversible** | `bool` |
| `add_column(table_name, column_name, column_type, constraints?)` | Add a column | `bool` |
| `drop_column(table_name, column_name)` | Remove a column — **irreversible** | `bool` |

### Export and Import

| Function | Description | Returns |
|---|---|---|
| `export_schema(tables?)` | Export column definitions + constraints as JSON | `str` (JSON) |
| `export_data(tables?, batch_size?)` | Export schema + all row data as JSON | `str` (JSON) |
| `import_schema(export_json, if_exists?)` | Recreate tables from an `export_schema` or `export_data` JSON string | `dict` |
| `import_data(export_json, if_exists?, truncate_before_insert?)` | Recreate tables and restore all rows from an `export_data` JSON string | `dict` |

**`if_exists` values (used by both import functions):**

| Value | Behaviour |
|---|---|
| `'skip'` | Leave existing tables untouched; still insert rows on `import_data` (default) |
| `'replace'` | DROP and recreate the table, then restore data |
| `'error'` | Raise an exception if the table already exists |

Both import functions return a summary dict:
```python
{
    "created":       ["table1", ...],
    "skipped":       ["table2", ...],
    "replaced":      ["table3", ...],
    "rows_inserted": {"table1": 42, "table2": 7, ...}  # import_data only
}
```

---

## Client-Side Examples

All functions are called from Anvil Forms using `anvil.server.call()`.

---

### READ

#### List all tables

```python
tables = anvil.server.call('get_tables')
print(tables)
# ['orders', 'products', 'users']
```

#### Inspect a table's columns

```python
columns = anvil.server.call('get_columns', 'users')
for col in columns:
    print(col['column'], '-', col['type'])
# id         - integer
# name       - text
# email      - text
# status     - text
# created_at - timestamp without time zone
```

#### Count rows

```python
# All rows
total = anvil.server.call('get_row_count', 'users')

# With a filter
active_count = anvil.server.call('get_row_count', 'users', {'status': 'active'})
```

#### Basic query

```python
result = anvil.server.call('query_table', 'users')

for user in result['rows']:
    print(user['name'], user['email'])
```

#### Query with filters

```python
result = anvil.server.call('query_table', 'users',
    filters={'status': 'active'}
)
```

#### Query with sorting

```python
# Ascending (default)
result = anvil.server.call('query_table', 'users',
    order_by='name'
)

# Descending
result = anvil.server.call('query_table', 'orders',
    order_by='created_at',
    order_dir='DESC'
)
```

#### Query with pagination

```python
result = anvil.server.call('query_table', 'users',
    page=2,
    page_size=20
)

print(f"Page {result['page']} of {result['total_pages']}")
print(f"Showing {len(result['rows'])} of {result['total']} total users")
```

#### Combined filter + sort + paginate

```python
result = anvil.server.call('query_table', 'users',
    filters={'status': 'active'},
    order_by='name',
    order_dir='ASC',
    page=1,
    page_size=25
)
```

#### Iterate through all pages

```python
all_rows = []
page = 1

while True:
    result = anvil.server.call('query_table', 'orders',
        order_by='created_at',
        order_dir='DESC',
        page=page,
        page_size=50
    )
    all_rows.extend(result['rows'])
    if page >= result['total_pages']:
        break
    page += 1

print(f"Fetched {len(all_rows)} total orders")
```

#### Fetch a single row by ID

```python
user = anvil.server.call('get_row_by_id', 'users', 42)

if user is None:
    print("User not found")
else:
    print(user['name'], user['email'])
```

---

### INSERT

#### Insert one row

```python
# Returns the full inserted row, including the auto-generated id
new_user = anvil.server.call('insert_row', 'users', {
    'name': 'Alice',
    'email': 'alice@example.com',
    'status': 'active'
})

print(f"Created user with id={new_user['id']}")
```

#### Bulk insert (atomic)

All rows are inserted in a single transaction. If any row fails, **none** are committed.

```python
rows = [
    {'name': 'Bob',     'email': 'bob@example.com',     'status': 'active'},
    {'name': 'Charlie', 'email': 'charlie@example.com', 'status': 'inactive'},
    {'name': 'Diana',   'email': 'diana@example.com',   'status': 'active'},
]

count = anvil.server.call('insert_many_rows', 'users', rows)
print(f"Inserted {count} users")
```

---

### UPDATE

#### Update one row by ID

```python
# Returns the full updated row
updated = anvil.server.call('update_row', 'users', 42, {
    'email': 'newemail@example.com',
    'status': 'inactive'
})

print(f"Updated: {updated['name']} → {updated['email']}")
```

#### Bulk update by condition

```python
# Reactivate all inactive users
count = anvil.server.call('update_rows_where', 'users',
    {'status': 'inactive'},  # WHERE
    {'status': 'active'}     # SET
)
print(f"Reactivated {count} users")

# Set a default currency on all existing orders
anvil.server.call('update_rows_where', 'orders',
    {'currency': None},
    {'currency': 'USD'}
)
```

---

### DELETE

#### Delete one row by ID

```python
deleted = anvil.server.call('delete_row', 'users', 42)

if deleted:
    print("User deleted")
else:
    print("User not found")
```

#### Delete rows by condition

```python
count = anvil.server.call('delete_rows_where', 'users',
    {'status': 'inactive'}
)
print(f"Removed {count} inactive users")
```

#### Truncate a table

```python
# Removes ALL rows — irreversible
anvil.server.call('delete_all_rows', 'session_logs')
```

---

### Table Management

#### Create a table

An `id SERIAL PRIMARY KEY` column is always prepended automatically.

```python
anvil.server.call('create_table', 'products', [
    {'name': 'title',      'type': 'TEXT',      'constraints': 'NOT NULL'},
    {'name': 'price',      'type': 'NUMERIC',   'constraints': 'DEFAULT 0'},
    {'name': 'in_stock',   'type': 'BOOLEAN',   'constraints': 'DEFAULT TRUE'},
    {'name': 'created_at', 'type': 'TIMESTAMP', 'constraints': "DEFAULT NOW()"},
])
```

**Supported PostgreSQL column types:**

| Type | Use for |
|---|---|
| `TEXT` | Names, descriptions, emails |
| `INTEGER` | Counts, foreign keys |
| `NUMERIC` | Prices, measurements |
| `BOOLEAN` | Flags, toggles |
| `TIMESTAMP` | Dates and times |
| `JSONB` | Nested structured data |
| `UUID` | Unique identifiers |

#### Drop a table

```python
anvil.server.call('drop_table', 'old_session_logs')
```

#### Add a column

```python
# Simple column
anvil.server.call('add_column', 'users', 'phone', 'TEXT')

# With a default value
anvil.server.call('add_column', 'users', 'verified', 'BOOLEAN', 'DEFAULT FALSE')

# NOT NULL with default
anvil.server.call('add_column', 'orders', 'currency', 'TEXT', "NOT NULL DEFAULT 'USD'")
```

#### Remove a column

```python
anvil.server.call('drop_column', 'users', 'legacy_token')
```

---

### Export and Import

#### Export schema only (no row data)

```python
# All tables
schema_json = anvil.server.call('export_schema')

# Specific tables only
schema_json = anvil.server.call('export_schema', ['users', 'orders'])

# Parse to inspect
import json
schema = json.loads(schema_json)
print(schema['exported_at'])
for table_name, table_def in schema['tables'].items():
    print(f"\n{table_name}")
    for col in table_def['columns']:
        print(f"  {col['column_name']}  {col['data_type']}")
```

#### Export schema + all data

```python
# All tables
dump_json = anvil.server.call('export_data')

# Specific tables
dump_json = anvil.server.call('export_data', ['users', 'orders'])

# Inspect row counts
import json
dump = json.loads(dump_json)
for table_name, table_def in dump['tables'].items():
    print(f"{table_name}: {len(table_def['rows'])} rows")
```

#### Import schema (recreate tables, no data)

```python
# Default: skip tables that already exist
result = anvil.server.call('import_schema', schema_json)
print(result)
# {'created': ['users', 'orders'], 'skipped': [], 'replaced': []}

# Drop and recreate tables that already exist
result = anvil.server.call('import_schema', schema_json, if_exists='replace')

# Raise an error if any table already exists
result = anvil.server.call('import_schema', schema_json, if_exists='error')
```

#### Import data (recreate tables + restore rows)

```python
# Default: skip schema creation for tables that exist, still insert rows
result = anvil.server.call('import_data', dump_json)
print(result)
# {
#   'created':  ['products'],
#   'skipped':  ['users'],
#   'replaced': [],
#   'rows_inserted': {'products': 150, 'users': 42}
# }

# Drop and fully restore — useful for disaster recovery
result = anvil.server.call('import_data', dump_json, if_exists='replace')

# Keep existing schema but clear and reload all rows
result = anvil.server.call('import_data', dump_json,
    if_exists='skip',
    truncate_before_insert=True
)
```

#### Full backup-and-restore pattern

```python
# --- BACKUP ---
dump_json = anvil.server.call('export_data')
# Save the string to a file, send it somewhere, store it, etc.
with open('backup.json', 'w') as f:
    f.write(dump_json)

# --- RESTORE to a clean database ---
with open('backup.json') as f:
    dump_json = f.read()

result = anvil.server.call('import_data', dump_json, if_exists='replace')
for table, count in result['rows_inserted'].items():
    print(f"  {table}: {count} rows restored")
```

#### Migrate data between two apps

```python
# On the source app — export
dump_json = anvil.server.call('export_data', ['users', 'products'])

# Transfer dump_json however you like (file, uplink, HTTP, etc.)

# On the target app — import
result = anvil.server.call('import_data', dump_json, if_exists='replace')
print(result['rows_inserted'])
```

---

## Error Handling

All server functions raise `anvil.server.CallError` on failure. Wrap calls in `try/except` in your Form code:

```python
try:
    new_user = anvil.server.call('insert_row', 'users', {
        'name': self.text_box_name.text,
        'email': self.text_box_email.text,
    })
    Notification("User created successfully.").show()

except anvil.server.CallError as e:
    Notification(f"Error: {e}", style="danger").show()
```

**Common error messages:**

| Message | Cause |
|---|---|
| `Table 'x' not found.` | Table doesn't exist in `app_tables` schema |
| `Unknown column(s) on 'x': ['y']` | Column name typo, or column doesn't exist |
| `Invalid table name 'x'.` | Table name contains invalid characters |
| `order_dir must be 'ASC' or 'DESC'.` | Invalid sort direction |
| `page must be >= 1.` | Page number out of range |
| `page_size must be between 1 and 1000.` | Page size out of range |
| `filters must not be empty.` | Called `delete_rows_where` or `update_rows_where` with `{}` |
| `data must not be empty.` | Called `insert_row` or `update_row` with `{}` |
| `Row with id=N not found in 'x'.` | `update_row` target ID doesn't exist |
| `All rows must have identical keys in the same order.` | Inconsistent dicts passed to `insert_many_rows` |
| `JSON does not look like an anvil-psql export.` | Wrong JSON passed to import functions |
| `export_type is 'schema', use import_schema() instead.` | Passed schema-only JSON to `import_data` |
| `Table 'x' already exists.` | `import_schema` / `import_data` called with `if_exists='error'` |
| `if_exists must be 'skip', 'replace', or 'error'.` | Invalid `if_exists` value |

### Exposing `_run_query` safely (admin only)

`_run_query` is intentionally not decorated with `@anvil.server.callable`. To expose it, create your own wrapper that enforces authentication:

```python
# In your Server Module
@anvil.server.callable(require_user=True)
def admin_run_query(sql_string, params=None):
    user = anvil.users.get_user()
    if not user or not user['is_admin']:
        raise Exception("Admin access required.")
    return _run_query(sql_string, params)
```

```python
# In your Form (admin users only)
rows = anvil.server.call('admin_run_query',
    "SELECT u.name, COUNT(o.id) AS orders FROM users u "
    "LEFT JOIN orders o ON o.user_id = u.id "
    "GROUP BY u.name ORDER BY orders DESC"
)
```

---

## Complete Working Form Example

A fully functional user management form with a data grid, search, pagination controls, add, edit, and delete.

```python
# Forms/UserManagement/__init__.py
import anvil.server
import anvil.users
from anvil import *
from ._anvil_designer import UserManagementTemplate


class UserManagement(UserManagementTemplate):

    def __init__(self, **properties):
        self.init_components(**properties)
        self.current_page = 1
        self.page_size = 20
        self.current_filters = {}
        self.current_order_by = 'name'
        self.current_order_dir = 'ASC'
        self.load_users()

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def load_users(self):
        try:
            result = anvil.server.call('query_table', 'users',
                filters=self.current_filters or None,
                order_by=self.current_order_by,
                order_dir=self.current_order_dir,
                page=self.current_page,
                page_size=self.page_size,
            )
        except anvil.server.CallError as e:
            Notification(f"Failed to load users: {e}", style="danger").show()
            return

        # Populate the repeating panel
        self.repeating_panel_users.items = result['rows']

        # Update pagination label
        self.label_page_info.text = (
            f"Page {result['page']} of {result['total_pages']}  "
            f"({result['total']} user{'s' if result['total'] != 1 else ''})"
        )

        # Enable/disable pagination buttons
        self.button_prev.enabled = self.current_page > 1
        self.button_next.enabled = self.current_page < result['total_pages']

        # Store total pages for bounds-checking
        self._total_pages = result['total_pages']

    # ------------------------------------------------------------------
    # Search / filter
    # ------------------------------------------------------------------

    def text_box_search_pressed_enter(self, **event_args):
        self._apply_search()

    def button_search_click(self, **event_args):
        self._apply_search()

    def _apply_search(self):
        query = self.text_box_search.text.strip()
        # Simple status filter — extend as needed
        self.current_filters = {'status': query} if query else {}
        self.current_page = 1
        self.load_users()

    def button_clear_search_click(self, **event_args):
        self.text_box_search.text = ''
        self.current_filters = {}
        self.current_page = 1
        self.load_users()

    # ------------------------------------------------------------------
    # Sorting (called from column header buttons)
    # ------------------------------------------------------------------

    def sort_by_column(self, column_name):
        if self.current_order_by == column_name:
            # Toggle direction if already sorting by this column
            self.current_order_dir = (
                'DESC' if self.current_order_dir == 'ASC' else 'ASC'
            )
        else:
            self.current_order_by = column_name
            self.current_order_dir = 'ASC'
        self.current_page = 1
        self.load_users()

    def button_sort_name_click(self, **event_args):
        self.sort_by_column('name')

    def button_sort_email_click(self, **event_args):
        self.sort_by_column('email')

    def button_sort_created_click(self, **event_args):
        self.sort_by_column('created_at')

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    def button_prev_click(self, **event_args):
        if self.current_page > 1:
            self.current_page -= 1
            self.load_users()

    def button_next_click(self, **event_args):
        if self.current_page < self._total_pages:
            self.current_page += 1
            self.load_users()

    # ------------------------------------------------------------------
    # Add user
    # ------------------------------------------------------------------

    def button_add_user_click(self, **event_args):
        name  = self.text_box_new_name.text.strip()
        email = self.text_box_new_email.text.strip()

        if not name or not email:
            Notification("Name and email are required.", style="warning").show()
            return

        try:
            new_user = anvil.server.call('insert_row', 'users', {
                'name':   name,
                'email':  email,
                'status': 'active',
            })
            Notification(f"Created user '{new_user['name']}' (id={new_user['id']}).").show()
            self.text_box_new_name.text  = ''
            self.text_box_new_email.text = ''
            self.load_users()

        except anvil.server.CallError as e:
            Notification(f"Could not create user: {e}", style="danger").show()

    # ------------------------------------------------------------------
    # Edit user (called from row component via raise_event / custom event)
    # ------------------------------------------------------------------

    def edit_user(self, row_id, new_data: dict):
        """Call this from a row component when the user saves an inline edit."""
        if not new_data:
            return
        try:
            updated = anvil.server.call('update_row', 'users', row_id, new_data)
            Notification(f"Updated '{updated['name']}'.").show()
            self.load_users()

        except anvil.server.CallError as e:
            Notification(f"Update failed: {e}", style="danger").show()

    # ------------------------------------------------------------------
    # Delete user (called from row component)
    # ------------------------------------------------------------------

    def delete_user(self, row_id, user_name: str):
        """Call this from a row component's delete button."""
        if not confirm(f"Delete '{user_name}'? This cannot be undone."):
            return
        try:
            deleted = anvil.server.call('delete_row', 'users', row_id)
            if deleted:
                Notification(f"Deleted '{user_name}'.").show()
            else:
                Notification("User not found.", style="warning").show()
            self.load_users()

        except anvil.server.CallError as e:
            Notification(f"Delete failed: {e}", style="danger").show()

    # ------------------------------------------------------------------
    # Bulk operations (admin only)
    # ------------------------------------------------------------------

    def button_deactivate_all_click(self, **event_args):
        if not confirm("Deactivate ALL active users?"):
            return
        try:
            count = anvil.server.call('update_rows_where', 'users',
                {'status': 'active'},
                {'status': 'inactive'},
            )
            Notification(f"Deactivated {count} users.").show()
            self.load_users()

        except anvil.server.CallError as e:
            Notification(f"Bulk update failed: {e}", style="danger").show()

    def button_purge_inactive_click(self, **event_args):
        count = anvil.server.call('get_row_count', 'users', {'status': 'inactive'})
        if not confirm(f"Permanently delete {count} inactive users?"):
            return
        try:
            deleted = anvil.server.call('delete_rows_where', 'users',
                {'status': 'inactive'}
            )
            Notification(f"Deleted {deleted} users.").show()
            self.load_users()

        except anvil.server.CallError as e:
            Notification(f"Bulk delete failed: {e}", style="danger").show()
```

### Corresponding row component

```python
# Forms/UserRow/__init__.py
import anvil.server
from anvil import *
from ._anvil_designer import UserRowTemplate


class UserRow(UserRowTemplate):
    """A single row in the users repeating panel."""

    def __init__(self, **properties):
        self.init_components(**properties)
        # 'item' is set automatically by the repeating panel
        self.label_name.text   = self.item['name']
        self.label_email.text  = self.item['email']
        self.label_status.text = self.item['status']

    def button_edit_click(self, **event_args):
        new_name  = anvil.alert(
            content=TextBox(text=self.item['name'], placeholder="Name"),
            title="Edit name",
            buttons=[("Save", True), ("Cancel", False)],
        )
        if new_name:
            # Bubble the edit up to the parent form
            self.parent.raise_event('x-edit-user',
                row_id=self.item['id'],
                new_data={'name': new_name.text},
            )

    def button_delete_click(self, **event_args):
        self.parent.raise_event('x-delete-user',
            row_id=self.item['id'],
            user_name=self.item['name'],
        )
```

### Wiring up custom events in the parent form

```python
# In UserManagement.__init__, after init_components:
self.repeating_panel_users.set_event_handler('x-edit-user',
    lambda row_id, new_data, **kw: self.edit_user(row_id, new_data)
)
self.repeating_panel_users.set_event_handler('x-delete-user',
    lambda row_id, user_name, **kw: self.delete_user(row_id, user_name)
)
```

---

## Complete Export / Import Form Example

A full backup/restore form with progress feedback, table selection, and error handling.

```python
# Forms/BackupRestore/__init__.py
import anvil.server
import anvil.media
import json
from anvil import *
from ._anvil_designer import BackupRestoreTemplate


class BackupRestore(BackupRestoreTemplate):

    def __init__(self, **properties):
        self.init_components(**properties)
        self._dump_json = None       # holds the last export in memory
        self.load_table_list()

    # ------------------------------------------------------------------
    # Startup — populate the table checkboxes
    # ------------------------------------------------------------------

    def load_table_list(self):
        try:
            tables = anvil.server.call('get_tables')
        except anvil.server.CallError as e:
            Notification(f"Could not load tables: {e}", style="danger").show()
            return

        self.check_box_panel.items = [
            {"table": t, "selected": True} for t in tables
        ]

    def _selected_tables(self) -> list[str] | None:
        """Return list of checked table names, or None if all are selected."""
        items = self.check_box_panel.items or []
        selected = [i["table"] for i in items if i.get("selected")]
        if len(selected) == len(items):
            return None         # None = all tables (slightly faster on server)
        return selected or None

    # ------------------------------------------------------------------
    # Export schema only
    # ------------------------------------------------------------------

    def button_export_schema_click(self, **event_args):
        self.label_status.text = "Exporting schema…"
        try:
            schema_json = anvil.server.call(
                'export_schema', self._selected_tables()
            )
            schema = json.loads(schema_json)
            table_count = len(schema["tables"])
            self.label_status.text = (
                f"Schema exported: {table_count} table(s) "
                f"at {schema['exported_at']}"
            )
            # Offer as a downloadable file
            anvil.media.download(
                anvil.BlobMedia(
                    "application/json",
                    schema_json.encode(),
                    name="schema_export.json",
                )
            )
        except anvil.server.CallError as e:
            self.label_status.text = f"Export failed: {e}"
            Notification(str(e), style="danger").show()

    # ------------------------------------------------------------------
    # Export schema + data
    # ------------------------------------------------------------------

    def button_export_data_click(self, **event_args):
        self.label_status.text = "Exporting data (this may take a moment)…"
        try:
            dump_json = anvil.server.call(
                'export_data', self._selected_tables()
            )
            dump = json.loads(dump_json)

            # Build a summary string
            summary_lines = []
            for table_name, table_def in dump["tables"].items():
                row_count = len(table_def.get("rows", []))
                summary_lines.append(f"  {table_name}: {row_count} rows")

            self.label_status.text = (
                f"Data exported at {dump['exported_at']}:\n"
                + "\n".join(summary_lines)
            )
            self._dump_json = dump_json      # keep in memory for quick restore

            anvil.media.download(
                anvil.BlobMedia(
                    "application/json",
                    dump_json.encode(),
                    name="data_export.json",
                )
            )
        except anvil.server.CallError as e:
            self.label_status.text = f"Export failed: {e}"
            Notification(str(e), style="danger").show()

    # ------------------------------------------------------------------
    # Import — user uploads a JSON file
    # ------------------------------------------------------------------

    def file_loader_change(self, file, **event_args):
        """Triggered when the user selects a .json file to import."""
        if file is None:
            return
        try:
            content = file.get_bytes().decode("utf-8")
            payload = json.loads(content)
        except Exception as e:
            Notification(f"Could not read file: {e}", style="danger").show()
            return

        export_type = payload.get("export_type", "unknown")
        table_count = len(payload.get("tables", {}))
        exported_at = payload.get("exported_at", "unknown")

        self.label_status.text = (
            f"Loaded {export_type} export ({table_count} tables, "
            f"exported {exported_at}). Choose an import mode below."
        )
        self._dump_json = content

    def _run_import(self, if_exists: str, truncate: bool = False):
        if not self._dump_json:
            Notification("No export loaded. Use Export Data first or upload a file.",
                         style="warning").show()
            return

        payload = json.loads(self._dump_json)
        export_type = payload.get("export_type")

        self.label_status.text = "Importing…"
        try:
            if export_type == "schema":
                result = anvil.server.call(
                    'import_schema', self._dump_json, if_exists
                )
                rows_info = ""
            else:
                result = anvil.server.call(
                    'import_data', self._dump_json, if_exists, truncate
                )
                rows_info = "\n" + "\n".join(
                    f"  {t}: {n} rows"
                    for t, n in result.get("rows_inserted", {}).items()
                )

            self.label_status.text = (
                f"Import complete.\n"
                f"  Created:  {result['created']}\n"
                f"  Skipped:  {result['skipped']}\n"
                f"  Replaced: {result['replaced']}"
                + rows_info
            )
            Notification("Import successful.", style="success").show()
            self.load_table_list()          # refresh table list

        except anvil.server.CallError as e:
            self.label_status.text = f"Import failed: {e}"
            Notification(str(e), style="danger").show()

    def button_import_skip_click(self, **event_args):
        """Import — skip tables that already exist."""
        self._run_import(if_exists="skip")

    def button_import_replace_click(self, **event_args):
        """Import — drop and recreate tables that already exist."""
        if not confirm(
            "This will DROP and recreate existing tables. All current data will be lost. Continue?"
        ):
            return
        self._run_import(if_exists="replace")

    def button_import_reload_click(self, **event_args):
        """Import — keep schema, truncate rows, then reload from export."""
        if not confirm(
            "This will TRUNCATE all matching tables before inserting. Continue?"
        ):
            return
        self._run_import(if_exists="skip", truncate=True)
```

**Suggested form layout:**

```
[ Export Schema ]  [ Export Data ]
─────────────────────────────────────
Tables to include:
  ☑ users   ☑ orders   ☑ products  …

─────────────────────────────────────
Import from file:  [ Upload JSON ]

[ Import (skip existing) ]
[ Import (replace existing) ]
[ Import (truncate & reload) ]

─────────────────────────────────────
Status:
  …
```

---

## Licence

MIT
