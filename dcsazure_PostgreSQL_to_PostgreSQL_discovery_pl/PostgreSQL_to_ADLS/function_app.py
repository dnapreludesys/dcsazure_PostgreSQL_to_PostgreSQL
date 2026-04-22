import gc
import json
import logging
import uuid
import defusedxml.ElementTree as ET
from collections import deque
from datetime import datetime
from io import StringIO
from typing import Dict, Iterator, List, Optional, Tuple

import azure.durable_functions as df
import azure.functions as func
import pandas as pd
import psycopg2
import psycopg2.extras
from azure.core.exceptions import ClientAuthenticationError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.filedatalake import DataLakeServiceClient

# ============================================================================
# CONSTANTS
# ============================================================================

DEFAULT_BATCH_SIZE = 500
ARRAY_PROCESSING_BATCH_SIZE = 2000

PIPE_DELIMITER = "|"
ESCAPE_CHARACTER = "\\"

DEFAULT_PG_PORT = 5432
DEFAULT_PG_SCHEMA = "public"
DEFAULT_PG_SSLMODE = "require"
DEFAULT_ADLS_DIRECTORY = ""
DEFAULT_CONNECT_TIMEOUT = 30

JSON_TYPES = {"json", "jsonb"}
XML_TYPES = {"xml"}

_LOG = logging.getLogger(__name__)


# ============================================================================
# AZURE FUNCTIONS APP
# ============================================================================

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


# ============================================================================
# KEY VAULT UTILITIES
# ============================================================================


def get_secret(key_vault_name: str, secret_name: str) -> str:
    """
    Retrieve a secret value from Azure Key Vault.

    Constructs the Key Vault URI from the supplied vault name, authenticates
    using DefaultAzureCredential (managed identity or environment credentials),
    and returns the plaintext secret value for the given secret name.
    """
    kv_uri = f"https://{key_vault_name}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=kv_uri, credential=credential)
    return client.get_secret(secret_name).value


# ============================================================================
# POSTGRESQL CONNECTION & METADATA UTILITIES
# ============================================================================


def get_postgres_connection(
    pg_host: str,
    pg_port: int,
    pg_database: str,
    pg_username: str,
    pg_password: str,
    pg_sslmode: str = DEFAULT_PG_SSLMODE,
):
    """
    Open and return a psycopg2 connection to a PostgreSQL database.

    Connects using the supplied host, port, database name, credentials, and
    SSL mode. A connect_timeout of DEFAULT_CONNECT_TIMEOUT seconds is applied.
    Raises ValueError with a descriptive message on OperationalError or any
    other unexpected exception.
    """
    try:
        return psycopg2.connect(
            host=pg_host,
            port=pg_port,
            dbname=pg_database,
            user=pg_username,
            password=pg_password,
            sslmode=pg_sslmode,
            connect_timeout=DEFAULT_CONNECT_TIMEOUT,
        )
    except psycopg2.OperationalError as e:
        raise ValueError(f"PostgreSQL connection failed: {str(e)}")
    except Exception as e:
        raise ValueError(f"Unexpected error connecting to PostgreSQL: {str(e)}")


def get_table_primary_keys(conn, schema: str, table: str) -> List[str]:
    """
    Return the ordered list of primary-key column names for a table.

    Queries information_schema to find columns belonging to the PRIMARY KEY
    constraint of the given schema.table, ordered by their ordinal position
    within the constraint. Returns an empty list if no primary key exists.
    """
    query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema   = kcu.table_schema
            AND tc.table_name     = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema    = %s
          AND tc.table_name      = %s
        ORDER BY kcu.ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def get_table_column_types(conn, schema: str, table: str) -> Dict[str, str]:
    """
    Return a mapping of column name to PostgreSQL data type for a table.

    Queries information_schema.columns for the specified schema.table and
    returns a dict of {column_name: data_type} with the data type lowercased.
    """
    query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name   = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        rows = cur.fetchall()
    return {r[0]: r[1].lower() for r in rows}


def validate_table_exists(conn, schema: str, table: str) -> None:
    """
    Raise ValueError if the specified table or view does not exist.

    Queries information_schema.tables for a BASE TABLE or VIEW matching the
    given schema and table name. If no matching row is found, raises ValueError
    with a descriptive message identifying the missing table.
    """
    query = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name   = %s
          AND table_type IN ('BASE TABLE', 'VIEW')
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        if not cur.fetchone():
            raise ValueError(f"Table '{table}' does not exist in schema '{schema}'.")


# ============================================================================
# STREAMING POSTGRESQL READER
# ============================================================================


class StreamingPostgresReader:
    """
    Stream rows from a PostgreSQL table in fixed-size batches.

    Supports two pagination strategies selected automatically based on whether
    the table has a primary key:
    - Keyset pagination (preferred): uses the last seen primary key values to
      seek forward, avoiding the performance degradation of large OFFSETs.
    - Offset pagination (fallback): used when no primary key exists.

    An optional filter_column / filter_values pair restricts rows to those
    whose filter_column value is in filter_values. Filtering is applied as a
    SQL IN clause and is only valid on scalar columns.
    """

    def __init__(
        self,
        conn,
        schema: str,
        table: str,
        pk_columns: List[str],
        batch_size: int,
        column_types: Dict[str, str],
        filter_column: Optional[str] = None,
        filter_values: Optional[List] = None,
    ):
        """
        Initialise the reader with connection details and pagination configuration.

        Sets _use_keyset to True when pk_columns is non-empty, which selects the
        keyset pagination strategy in stream_rows().
        """
        self.conn = conn
        self.schema = schema
        self.table = table
        self.pk_columns = pk_columns
        self.batch_size = batch_size
        self.column_types = column_types
        self.filter_column = filter_column
        self.filter_values = filter_values
        self.batches_fetched = 0
        self.rows_fetched = 0
        self._use_keyset = bool(pk_columns)

    def stream_rows(self) -> Iterator[List[Dict]]:
        """
        Yield successive batches of rows as lists of dicts.

        Delegates to _stream_keyset() when a primary key is available, or
        _stream_offset() otherwise. Each batch is a list of row dicts where
        keys are column names and values are the raw psycopg2-decoded values.
        """
        if self._use_keyset:
            yield from self._stream_keyset()
        else:
            yield from self._stream_offset()

    def _build_filter_clause(self) -> Tuple[str, list]:
        """
        Build the SQL WHERE fragment and parameter list for the optional column filter.

        Returns an empty string and empty list when no filter is configured.
        Otherwise returns a '<col> IN (%s, ...)' string and the corresponding
        list of filter values for use as psycopg2 query parameters.
        """
        if not self.filter_column or not self.filter_values:
            return "", []
        quoted_col = f'"{self.filter_column}"'
        placeholders = ", ".join(["%s"] * len(self.filter_values))
        return f"{quoted_col} IN ({placeholders})", list(self.filter_values)

    def _stream_keyset(self) -> Iterator[List[Dict]]:
        """
        Stream rows using keyset (seek) pagination ordered by primary key columns.

        Issues the base query (no keyset predicate) for the first batch, then
        appends a tuple-comparison WHERE clause using the last seen PK values
        for each subsequent batch. Stops when a batch returns fewer rows than
        batch_size or returns no rows. Calls gc.collect() between batches to
        release cursor memory.
        """
        quoted_schema = f'"{self.schema}"'
        quoted_table = f'"{self.table}"'
        quoted_pks = [f'"{c}"' for c in self.pk_columns]
        order_clause = ", ".join(quoted_pks)
        pk_tuple = f"({', '.join(quoted_pks)})"
        pk_placeholders = ", ".join(["%s"] * len(self.pk_columns))
        keyset_clause = f"{pk_tuple} > ({pk_placeholders})"

        filter_sql, filter_params = self._build_filter_clause()

        if filter_sql:
            base_query = (
                f"SELECT * FROM {quoted_schema}.{quoted_table} WHERE {filter_sql} "
                f"ORDER BY {order_clause} LIMIT %s"
            )
            keyset_query = (
                f"SELECT * FROM {quoted_schema}.{quoted_table} WHERE {filter_sql} AND {keyset_clause} "
                f"ORDER BY {order_clause} LIMIT %s"
            )
        else:
            base_query = f"SELECT * FROM {quoted_schema}.{quoted_table} ORDER BY {order_clause} LIMIT %s"
            keyset_query = (
                f"SELECT * FROM {quoted_schema}.{quoted_table} WHERE {keyset_clause} "
                f"ORDER BY {order_clause} LIMIT %s"
            )

        last_pk_values = None
        while True:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if last_pk_values is None:
                    cur.execute(base_query, (*filter_params, self.batch_size))
                else:
                    cur.execute(
                        keyset_query, (*filter_params, *last_pk_values, self.batch_size)
                    )
                rows = cur.fetchall()

            if not rows:
                break
            batch = [dict(r) for r in rows]
            self.batches_fetched += 1
            self.rows_fetched += len(batch)
            yield batch
            if len(batch) < self.batch_size:
                break
            last_pk_values = tuple(batch[-1][pk] for pk in self.pk_columns)
            gc.collect()

    def _stream_offset(self) -> Iterator[List[Dict]]:
        """
        Stream rows using LIMIT / OFFSET pagination.

        Used as a fallback when the table has no primary key. Increments the
        offset by the number of rows returned each batch. Stops when a batch
        returns fewer rows than batch_size or returns no rows. Calls
        gc.collect() between batches.
        """
        quoted_schema = f'"{self.schema}"'
        quoted_table = f'"{self.table}"'
        offset = 0
        filter_sql, filter_params = self._build_filter_clause()
        if filter_sql:
            query = f"SELECT * FROM {quoted_schema}.{quoted_table} WHERE {filter_sql} LIMIT %s OFFSET %s"
        else:
            query = f"SELECT * FROM {quoted_schema}.{quoted_table} LIMIT %s OFFSET %s"

        while True:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (*filter_params, self.batch_size, offset))
                rows = cur.fetchall()
            if not rows:
                break
            batch = [dict(r) for r in rows]
            self.batches_fetched += 1
            self.rows_fetched += len(batch)
            yield batch
            if len(batch) < self.batch_size:
                break
            offset += len(batch)
            gc.collect()

    def get_stats(self) -> Dict:
        """
        Return a summary dict of fetch statistics for this reader instance.

        Includes the total number of batches fetched, total rows fetched, and
        the pagination strategy used ('keyset' or 'offset').
        """
        return {
            "batches_fetched": self.batches_fetched,
            "rows_fetched": self.rows_fetched,
            "pagination_strategy": "keyset" if self._use_keyset else "offset",
        }


# ============================================================================
# JSON PARSING
# ============================================================================


def parse_json_value(value):
    """
    Parse a JSON/JSONB column value into a Python dict or list.

    psycopg2 auto-deserialises JSONB to Python objects, so value may already
    be a dict or list and is returned as-is. Strings are parsed via json.loads.
    Returns the parsed object (dict or list), or None if the value is None,
    empty, or cannot be parsed. Non-dict/non-list parsed values (e.g. bare
    numbers or strings) are also returned as None.
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, (dict, list)) else None
        except Exception:
            return None
    return None


# ============================================================================
# XML PARSING
# ============================================================================


def parse_xml_value(value) -> Optional[Dict]:
    """
    Parse an XML string column value into a nested Python dict.

    Uses defusedxml to safely parse the XML string, then converts the root
    element recursively via _xml_element_to_dict. Adds a '_xml_root_tag_' key
    to the result dict so that the reconstruction side (adls_to_postgres) can
    use the original root element tag instead of the column name when
    rebuilding the XML document. Returns None if the value is None or parsing
    fails for any reason.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    try:
        root = ET.fromstring(value.strip())
        result = _xml_element_to_dict(root)
        result["_xml_root_tag_"] = root.tag
        return result
    except Exception:
        return None


def _xml_element_to_dict(element) -> Dict:
    """
    Convert a defusedxml Element recursively into a nested Python dict.

    The element parameter is typed as Any (not ET.Element) because defusedxml
    does not re-export the Element class; the object returned by fromstring()
    is the stdlib xml.etree.ElementTree.Element, but importing that class
    directly from stdlib is intentionally avoided.

    Rules applied during conversion:
    - Element attributes become '@<attr>' keys in the result dict.
    - A single child element is stored as a nested dict (complex) or a string
      scalar (leaf with no children or attributes).
    - Repeated sibling elements sharing the same tag:
        * All text-only siblings are stored as a Python list of strings. The
          caller (_extract_arrays_from_dict) serialises primitive lists to a
          JSON string so they survive the CSV round-trip and can be re-expanded
          to sibling elements during XML reconstruction.
        * Any complex siblings (with children or attributes) are stored as a
          list of dicts and treated as a nested array by _extract_arrays_from_dict.
    - A text node with no child elements is stored under the '_text' key for
      mixed-content elements.
    """
    result: Dict = {}

    for attr_name, attr_value in element.attrib.items():
        result[f"@{attr_name}"] = attr_value

    children_by_tag: Dict[str, list] = {}
    for child in element:
        children_by_tag.setdefault(child.tag, []).append(child)

    for tag, children in children_by_tag.items():
        all_text_only = all(len(c) == 0 and not c.attrib for c in children)

        if len(children) == 1:
            child = children[0]
            if len(child) == 0 and not child.attrib:
                result[tag] = (child.text or "").strip()
            else:
                result[tag] = _xml_element_to_dict(child)
        else:
            if all_text_only:
                result[tag] = [(c.text or "").strip() for c in children]
            else:
                result[tag] = [_xml_element_to_dict(c) for c in children]

    text = (element.text or "").strip()
    if text and not children_by_tag:
        result["_text"] = text

    return result


# ============================================================================
# ARRAY EXTRACTION
# ============================================================================


def _extract_arrays_from_dict(
    doc: Dict,
    parent_rid: str,
    table_prefix: str,
) -> Tuple[Dict, Dict[str, List[Dict]]]:
    """
    Extract nested arrays of objects from a parsed JSON/XML dict into child
    table records, leaving scalar fields and primitive-array JSON strings in
    the returned parent_fields dict.

    Flattens the input dict using dot-separated keys (stopping at list values).
    For each key whose value is a list:
    - Empty lists are stored as the JSON string '[]'.
    - Primitive lists (no dict items) are JSON-serialised to a string so they
      survive the CSV round-trip and can be re-parsed on the ADLS→PG side.
    - Complex lists (containing at least one dict) are queued as child table
      records. A '_has_array_<key>' marker is placed in parent_fields.
      Primitive items in mixed lists are skipped.

    Child records are processed breadth-first via a deque. Each child record
    receives a new UUID '_rid' and the '_parent_rid' of its immediate parent
    (not the top-level entity). Child batches are flushed to child_records when
    they reach ARRAY_PROCESSING_BATCH_SIZE rows.

    Returns (parent_fields dict, child_records dict mapping table name to rows).
    """
    parent_fields: Dict = {}
    child_records: Dict[str, List[Dict]] = {}

    queue = deque()

    def flatten_no_arrays(d: Dict, prefix: str = "") -> Dict:
        """
        Flatten a dict into dot-separated keys, preserving list values intact.

        Recurses into nested dicts, building dot-separated keys. List values
        are kept as-is at their flattened key for classification by the caller.
        """
        flat = {}
        for k, v in d.items():
            key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, list):
                flat[key] = v
            elif isinstance(v, dict):
                flat.update(flatten_no_arrays(v, key))
            else:
                flat[key] = v
        return flat

    def _classify_list(lst: list) -> str:
        """
        Return 'complex' if any item in the list is a dict, else 'primitive'.

        Used to decide whether a list should be JSON-serialised in place
        (primitive) or extracted into a child table (complex).
        """
        for item in lst:
            if isinstance(item, dict):
                return "complex"
        return "primitive"

    flat_root = flatten_no_arrays(doc)

    for key, value in flat_root.items():
        if not isinstance(value, list):
            parent_fields[key] = value
            continue

        if not value:
            parent_fields[key] = "[]"
            continue

        kind = _classify_list(value)

        if kind == "primitive":
            parent_fields[key] = json.dumps(value, ensure_ascii=False, default=str)
        else:
            parent_fields[f"_has_array_{key}"] = True
            for item in value:
                if not isinstance(item, dict):
                    continue
                item_rid = str(uuid.uuid4())
                item["_rid"] = item_rid
                item["_parent_rid"] = parent_rid
                queue.append((item, f"{table_prefix}.{key}", parent_rid))

    current_batch: Dict[str, List[Dict]] = {}

    while queue:
        current, table_name, par_rid = queue.popleft()
        flat = flatten_no_arrays(current)
        row = {"_rid": current["_rid"], "_parent_rid": par_rid}

        for key, value in flat.items():
            if not isinstance(value, list):
                row[key] = value
                continue

            if not value:
                row[key] = "[]"
                continue

            kind = _classify_list(value)
            if kind == "primitive":
                row[key] = json.dumps(value, ensure_ascii=False, default=str)
            else:
                row[f"_has_array_{key}"] = True
                for item in value:
                    if not isinstance(item, dict):
                        continue
                    item_rid = str(uuid.uuid4())
                    item["_rid"] = item_rid
                    item["_parent_rid"] = current["_rid"]
                    queue.append((item, f"{table_name}.{key}", current["_rid"]))

        current_batch.setdefault(table_name, []).append(row)

        if len(current_batch.get(table_name, [])) >= ARRAY_PROCESSING_BATCH_SIZE:
            child_records.setdefault(table_name, []).extend(current_batch[table_name])
            current_batch[table_name] = []

    for table_name, rows in current_batch.items():
        if rows:
            child_records.setdefault(table_name, []).extend(rows)

    return parent_fields, child_records


# ============================================================================
# STRUCTURED COLUMN EXTRACTION
# ============================================================================


def extract_structured_columns(
    row: Dict,
    column_types: Dict[str, str],
    row_rid: str,
) -> Tuple[Dict, Dict[str, List[Dict]]]:
    """
    Separate a PostgreSQL row into scalar parent fields and structured child records.

    Iterates each column in the row and handles it based on its data type:

    For JSON/JSONB columns:
    - Attempts to parse the value via parse_json_value.
    - On success: adds 'has_json_<col> = True' to parent_fields, generates a
      new child_row_rid, calls _extract_arrays_from_dict with that rid as the
      parent_rid (ensuring nested children are wired to their immediate parent
      row, not the top-level entity rid), and stores the resulting child row
      plus any nested child records.
    - On failure: adds 'has_json_<col> = False' and keeps the raw value in
      parent_fields.

    For XML columns: identical logic using parse_xml_value and 'has_xml_<col>'.

    For all other column types: the value is passed through unchanged into
    parent_fields.

    Returns (parent_fields dict, child_records dict mapping table name to rows).
    """
    parent_fields: Dict = {}
    child_records: Dict[str, List[Dict]] = {}

    for col, value in row.items():
        col_type = column_types.get(col, "")

        if col_type in JSON_TYPES:
            parsed = parse_json_value(value)
            if parsed is not None:
                doc = {"_items": parsed} if isinstance(parsed, list) else parsed
                parent_fields[f"has_json_{col}"] = True
                child_row_rid = str(uuid.uuid4())
                child_parent_fields, nested_children = _extract_arrays_from_dict(
                    doc, parent_rid=child_row_rid, table_prefix=col
                )
                child_row = {"_rid": child_row_rid, "_parent_rid": row_rid}
                child_row.update(child_parent_fields)
                child_records.setdefault(col, []).append(child_row)
                for nested_name, nested_rows in nested_children.items():
                    child_records.setdefault(nested_name, []).extend(nested_rows)
            else:
                parent_fields[f"has_json_{col}"] = False
                parent_fields[col] = value

        elif col_type in XML_TYPES:
            parsed = parse_xml_value(value)
            if parsed is not None:
                parent_fields[f"has_xml_{col}"] = True
                child_row_rid = str(uuid.uuid4())
                child_parent_fields, nested_children = _extract_arrays_from_dict(
                    parsed, parent_rid=child_row_rid, table_prefix=col
                )
                child_row = {"_rid": child_row_rid, "_parent_rid": row_rid}
                child_row.update(child_parent_fields)
                child_records.setdefault(col, []).append(child_row)
                for nested_name, nested_rows in nested_children.items():
                    child_records.setdefault(nested_name, []).extend(nested_rows)
            else:
                parent_fields[f"has_xml_{col}"] = False
                parent_fields[col] = value

        else:
            parent_fields[col] = value

    return parent_fields, child_records


# ============================================================================
# DATA CLEANING
# ============================================================================


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean a DataFrame before uploading to ADLS.

    Applies fix_value to every cell in the DataFrame. The per-cell rules are:
    - None is preserved as None.
    - str, int, float, bool are returned unchanged.
    - Python lists are JSON-serialised to a string (rather than being dropped
      to None), as _extract_arrays_from_dict should have already serialised
      all primitive lists but this handles any residual cases defensively.
    - Unexpected dicts (which should have been extracted into child tables) are
      converted to None.
    - All other types are converted to str; if that fails, None is returned.

    Returns an empty DataFrame if the input is None or empty.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    def fix_value(val):
        if val is None:
            return None
        if isinstance(val, (str, int, float, bool)):
            return val
        if isinstance(val, list):
            try:
                return json.dumps(val, ensure_ascii=False, default=str)
            except Exception:
                return str(val)
        if isinstance(val, dict):
            return None
        try:
            return str(val)
        except Exception:
            return None

    for col in df.columns:
        df[col] = df[col].apply(fix_value)

    return df


# ============================================================================
# CHILD TABLE ADLS PATH DERIVATION
# ============================================================================


def _child_adls_path(table_dir: str, child_name: str) -> Tuple[str, str]:
    """
    Return (adls_directory, filename) for a child table CSV.

    Derives the path so that adls_to_postgres._organize_csv_paths_by_depth
    can correctly parse it back to the original col_name. That function does:

        rel      = path[len(table_dir) + 1:]
        parts    = [p for p in rel.split("/") if p]
        col_name = ".".join(parts[:-1]) if len(parts) > 1
                   else parts[0].rsplit(".", 1)[0]

    For col_name to equal child_name (e.g. "pref.loyalty.history") the layout
    must be:

        dir  = <table_dir>/<seg1>/<seg2>/.../<seg_n>
        file = <seg_n>.csv

    All dot-segments of child_name become subdirectory levels, and the last
    segment is also used as the filename (repeated), so that joining all
    parts[:-1] with '.' recreates the full child_name.
    """
    parts = child_name.split(".")
    dir_path = f"{table_dir}/{'/'.join(parts)}"
    file_name = f"{parts[-1]}.csv"
    return dir_path, file_name


# ============================================================================
# ADLS FILE UPLOAD
# ============================================================================


def upload_csv_to_adls(
    service_client,
    file_system: str,
    directory: str,
    file_name: str,
    df: pd.DataFrame,
    mode: str = "write",
    known_columns: Optional[set] = None,
) -> set:
    """
    Upload a DataFrame as a pipe-delimited CSV file to ADLS Gen2.

    Creates all intermediate directory segments if they do not exist. Two
    write modes are supported:

    'write': Deletes any existing file and writes the DataFrame from scratch
    with a header row.

    'append': Appends the DataFrame to an existing file without repeating
    the header. If new columns appear in the current batch that were not
    present in prior batches (schema evolution), the existing file content
    is read back, the new columns are added (as None) to both the existing
    and new data, both DataFrames are sorted to a consistent column order,
    concatenated, and the file is replaced entirely.

    In both modes, columns are sorted alphabetically using known_columns to
    ensure consistent column ordering across batches. Returns the updated
    set of known columns (union of prior and current batch columns).

    Raises on upload failures after logging the error.
    """
    df = clean_dataframe(df)
    if df is None or df.empty:
        return known_columns or set()

    fs_client = service_client.get_file_system_client(file_system)

    dir_segments = directory.strip("/").split("/") if directory else []
    curr = ""
    for seg in dir_segments:
        curr = f"{curr}/{seg}" if curr else seg
        try:
            fs_client.get_directory_client(curr).create_directory()
        except Exception:
            pass

    dir_client = fs_client.get_directory_client(directory)
    file_client = dir_client.get_file_client(file_name)

    current_columns = set(df.columns)
    if known_columns is None:
        known_columns = current_columns

    new_columns = current_columns - known_columns

    if mode == "append" and new_columns:
        try:
            properties = file_client.get_file_properties()
            if properties.size > 0:
                download = file_client.download_file()
                existing_content = download.readall().decode("utf-8")
                existing_df = pd.read_csv(
                    StringIO(existing_content),
                    sep=PIPE_DELIMITER,
                    keep_default_na=False,
                )
                for col in new_columns:
                    existing_df[col] = None
                known_columns = known_columns | new_columns
                for col in known_columns:
                    if col not in df.columns:
                        df[col] = None
                column_order = sorted(known_columns)
                existing_df = existing_df[column_order]
                df = df[column_order]
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                csv_buffer = StringIO()
                combined_df.to_csv(
                    csv_buffer,
                    index=False,
                    sep=PIPE_DELIMITER,
                    header=True,
                    quoting=0,
                    escapechar=ESCAPE_CHARACTER,
                    na_rep="",
                )
                content = csv_buffer.getvalue()
                file_client.delete_file()
                file_client = dir_client.create_file(file_name)
                content_bytes = content.encode("utf-8")
                file_client.append_data(
                    content_bytes, offset=0, length=len(content_bytes)
                )
                file_client.flush_data(len(content_bytes))
                return known_columns
        except Exception:
            pass

    for col in known_columns:
        if col not in df.columns:
            df[col] = None

    if known_columns:
        df = df[sorted(known_columns)]

    try:
        if mode == "write":
            try:
                file_client.delete_file()
            except Exception:
                pass
            csv_buffer = StringIO()
            df.to_csv(
                csv_buffer,
                index=False,
                sep=PIPE_DELIMITER,
                header=True,
                quoting=0,
                escapechar=ESCAPE_CHARACTER,
                na_rep="",
            )
            content = csv_buffer.getvalue()
            file_client = dir_client.create_file(file_name)
            content_bytes = content.encode("utf-8")
            file_client.append_data(content_bytes, offset=0, length=len(content_bytes))
            file_client.flush_data(len(content_bytes))
        else:
            file_exists = False
            current_size = 0
            try:
                properties = file_client.get_file_properties()
                current_size = properties.size
                file_exists = True
            except Exception:
                file_exists = False

            csv_buffer = StringIO()
            df.to_csv(
                csv_buffer,
                index=False,
                sep=PIPE_DELIMITER,
                header=not file_exists,
                quoting=0,
                escapechar=ESCAPE_CHARACTER,
                na_rep="",
            )
            content = csv_buffer.getvalue()
            if not file_exists:
                file_client = dir_client.create_file(file_name)
                current_size = 0
            content_bytes = content.encode("utf-8")
            file_client.append_data(
                content_bytes, offset=current_size, length=len(content_bytes)
            )
            file_client.flush_data(current_size + len(content_bytes))

    except Exception as e:
        _LOG.error(f"Upload failed for {file_name}: {str(e)}")
        raise

    return known_columns | current_columns


# ============================================================================
# ADLS CONNECTION VALIDATION
# ============================================================================


def validate_adls_connection(
    adls_account_name: str,
    adls_file_system: str,
    adls_account_key: str,
) -> DataLakeServiceClient:
    """
    Validate connectivity to an Azure Data Lake Storage Gen2 filesystem.

    Creates a DataLakeServiceClient authenticated with the provided account key,
    then performs a lightweight path listing (max 1 result) to confirm the
    filesystem exists and credentials are accepted. Returns the authenticated
    service client for subsequent operations. Raises ValueError with a
    descriptive message for authentication failures, missing filesystems, or
    any other connection error.
    """
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{adls_account_name}.dfs.core.windows.net",
            credential=adls_account_key,
        )
        fs_client = service_client.get_file_system_client(adls_file_system)
        list(fs_client.get_paths(path="", max_results=1))
        return service_client
    except ClientAuthenticationError:
        raise ValueError(
            "ADLS authentication failed. Check the account key stored in Key Vault."
        )
    except ResourceNotFoundError:
        raise ValueError(f"ADLS filesystem '{adls_file_system}' does not exist.")
    except Exception as e:
        raise ValueError(f"ADLS connection validation failed: {str(e)}")


# ============================================================================
# PARAMETER VALIDATION
# ============================================================================


def validate_and_extract_params(params: dict) -> dict:
    """
    Validate the input parameter dict and return a normalised copy.

    Checks that all required keys are present and non-empty. Validates that
    filter_column and filter_value are either both provided or both absent,
    and that filter_value is non-empty when supplied. Coerces pg_port and
    batch_size to integers, falling back to DEFAULT_PG_PORT and
    DEFAULT_BATCH_SIZE respectively on parse failure. Applies defaults for
    optional keys: pg_schema, pg_sslmode, and adls_directory. Raises ValueError
    with descriptive messages for any validation failure.
    """
    required = {
        "pg_host": params.get("pg_host"),
        "pg_database": params.get("pg_database"),
        "pg_username": params.get("pg_username"),
        "pg_secret_name": params.get("pg_secret_name"),
        "key_vault_name": params.get("key_vault_name"),
        "table": params.get("table"),
        "adls_account_name": params.get("adls_account_name"),
        "adls_file_system": params.get("adls_file_system"),
        "adls_secret_name": params.get("adls_secret_name"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing required parameters: {missing}")

    table = params["table"]
    if not isinstance(table, str) or not table.strip():
        raise ValueError("'table' must be a non-empty string.")
    table = table.strip()

    filter_column = params.get("filter_column")
    filter_value = params.get("filter_value")
    has_col = bool(filter_column and str(filter_column).strip())
    has_val = filter_value is not None and filter_value != []
    if has_col != has_val:
        raise ValueError(
            "Both 'filter_column' and 'filter_value' must be provided together."
        )

    if has_col:
        filter_column = str(filter_column).strip()
        filter_values = [
            v
            for v in (
                filter_value if isinstance(filter_value, list) else [filter_value]
            )
            if v is not None
        ]
        if not filter_values:
            raise ValueError("'filter_value' was provided but is empty.")
    else:
        filter_column = None
        filter_values = None

    try:
        pg_port = int(params.get("pg_port", DEFAULT_PG_PORT))
    except (ValueError, TypeError):
        pg_port = DEFAULT_PG_PORT

    batch_size = params.get("batch_size", DEFAULT_BATCH_SIZE)
    try:
        batch_size = max(1, int(batch_size))
    except (ValueError, TypeError):
        batch_size = DEFAULT_BATCH_SIZE

    return {
        "pg_host": params["pg_host"],
        "pg_port": pg_port,
        "pg_database": params["pg_database"],
        "pg_username": params["pg_username"],
        "pg_secret_name": params["pg_secret_name"],
        "key_vault_name": params["key_vault_name"],
        "pg_schema": params.get("pg_schema", DEFAULT_PG_SCHEMA),
        "pg_sslmode": params.get("pg_sslmode", DEFAULT_PG_SSLMODE),
        "table": table,
        "filter_column": filter_column,
        "filter_values": filter_values,
        "adls_account_name": params["adls_account_name"],
        "adls_file_system": params["adls_file_system"],
        "adls_secret_name": params["adls_secret_name"],
        "adls_directory": params.get("adls_directory", DEFAULT_ADLS_DIRECTORY),
        "batch_size": batch_size,
    }


# ============================================================================
# PER-TABLE PROCESSING
# ============================================================================


def process_table(
    conn,
    service_client,
    schema: str,
    table: str,
    batch_size: int,
    adls_file_system: str,
    export_dir: str,
    filter_column: Optional[str] = None,
    filter_values: Optional[List] = None,
) -> Dict:
    """
    Export a single PostgreSQL table to ADLS as a set of pipe-delimited CSV files.

    Introspects the table for column types and primary keys. Validates that the
    optional filter_column exists and is not a JSON/XML column. Creates a
    StreamingPostgresReader to iterate rows in batches.

    For each batch:
    - Assigns a UUID '_rid' to each row.
    - Calls extract_structured_columns to split the row into scalar parent
      fields and structured child records (one child table per JSON/XML column,
      with further nesting for array-valued sub-fields).
    - Uploads the parent records to '<table_dir>/<table>.csv' (write on the
      first batch, append on subsequent batches).
    - Uploads each child table's records to the path returned by
      _child_adls_path, maintaining per-child schema state across batches for
      correct schema-evolution handling.
    - Frees batch memory and calls gc.collect() after each batch.

    Returns a summary dict with table name, rows processed, batch count,
    child table names, pagination strategy, and pk_columns.
    """
    column_types = get_table_column_types(conn, schema, table)
    pk_columns = get_table_primary_keys(conn, schema, table)

    if not pk_columns:
        _LOG.warning(
            f"Table '{schema}.{table}' has no primary key. "
            "Falling back to LIMIT/OFFSET pagination."
        )

    if filter_column:
        if filter_column not in column_types:
            raise ValueError(
                f"filter_column '{filter_column}' does not exist in table '{table}'."
            )
        if column_types[filter_column] in JSON_TYPES | XML_TYPES:
            raise ValueError(
                f"filter_column '{filter_column}' is a {column_types[filter_column]} column. "
                "Filtering is only supported on scalar columns."
            )

    reader = StreamingPostgresReader(
        conn=conn,
        schema=schema,
        table=table,
        pk_columns=pk_columns,
        batch_size=batch_size,
        column_types=column_types,
        filter_column=filter_column,
        filter_values=filter_values,
    )

    table_dir = f"{export_dir}/{table}"
    parent_file = f"{table}.csv"

    rows_processed = 0
    batch_num = 0
    parent_schema: Optional[set] = None
    child_schemas: Dict[str, Optional[set]] = {}
    child_table_names: set = set()

    for batch_rows in reader.stream_rows():
        batch_num += 1
        parent_records = []
        batch_child_records: Dict[str, List[Dict]] = {}

        for row in batch_rows:
            row_rid = str(uuid.uuid4())
            row["_rid"] = row_rid
            parent_fields, child_records = extract_structured_columns(
                row, column_types, row_rid
            )
            parent_records.append(parent_fields)
            for child_name, child_rows in child_records.items():
                batch_child_records.setdefault(child_name, []).extend(child_rows)

        parent_df = pd.DataFrame(parent_records)
        mode = "append" if batch_num > 1 else "write"
        parent_schema = upload_csv_to_adls(
            service_client,
            adls_file_system,
            table_dir,
            parent_file,
            parent_df,
            mode,
            parent_schema,
        )

        for child_name, child_rows in batch_child_records.items():
            child_table_names.add(child_name)
            child_df = pd.DataFrame(child_rows)
            child_df = clean_dataframe(child_df)
            if child_df is None or child_df.empty:
                continue

            child_dir, child_file = _child_adls_path(table_dir, child_name)
            child_mode = "append" if child_name in child_schemas else "write"
            if child_name not in child_schemas:
                child_schemas[child_name] = None
            child_schemas[child_name] = upload_csv_to_adls(
                service_client,
                adls_file_system,
                child_dir,
                child_file,
                child_df,
                child_mode,
                child_schemas[child_name],
            )

        rows_processed += len(batch_rows)
        del parent_df, parent_records, batch_child_records, batch_rows
        gc.collect()

    return {
        "table": table,
        "rows_processed": rows_processed,
        "batches": batch_num,
        "child_tables": list(child_table_names),
        "pagination": reader.get_stats()["pagination_strategy"],
        "pk_columns": pk_columns,
    }


# ============================================================================
# MAIN ACTIVITY FUNCTION
# ============================================================================


@app.activity_trigger(input_name="params")
def process_postgres_to_adls_activity(params: dict):
    """
    Azure Durable Functions activity that exports a PostgreSQL table to ADLS.

    Orchestrates the full export pipeline for a single table:
    1. Validates and extracts parameters via validate_and_extract_params.
    2. Retrieves the PostgreSQL password and ADLS account key from Azure Key Vault.
    3. Opens a PostgreSQL connection and validates ADLS connectivity.
    4. Validates that the target table exists.
    5. Constructs the ADLS export directory path from database and schema names.
    6. Calls process_table to stream and upload all rows.
    7. Returns a summary dict with status, row counts, batch count, child table
       names, pagination strategy, filter details, and duration.

    Returns a dict with 'status' key set to 'success' or 'error', along with
    relevant metrics or an error message.
    """
    start_time = datetime.utcnow()
    conn = None

    try:
        params = validate_and_extract_params(params)
        pg_password = get_secret(params["key_vault_name"], params["pg_secret_name"])
        conn = get_postgres_connection(
            params["pg_host"],
            params["pg_port"],
            params["pg_database"],
            params["pg_username"],
            pg_password,
            params["pg_sslmode"],
        )
        validate_table_exists(conn, params["pg_schema"], params["table"])
        adls_account_key = get_secret(
            params["key_vault_name"], params["adls_secret_name"]
        )
        service_client = validate_adls_connection(
            params["adls_account_name"],
            params["adls_file_system"],
            adls_account_key,
        )

        export_dir = (
            f"{params['adls_directory']}/{params['pg_database']}/{params['pg_schema']}"
            if params["adls_directory"]
            else f"{params['pg_database']}/{params['pg_schema']}"
        ).strip("/")

        table = params["table"]
        _LOG.info(f"Processing table: {params['pg_schema']}.{table}")

        result = process_table(
            conn=conn,
            service_client=service_client,
            schema=params["pg_schema"],
            table=table,
            batch_size=params["batch_size"],
            adls_file_system=params["adls_file_system"],
            export_dir=export_dir,
            filter_column=params["filter_column"],
            filter_values=params["filter_values"],
        )

        if result["rows_processed"] == 0:
            return {
                "status": "success",
                "message": f"No rows found in table '{table}'.",
                "rows_processed": 0,
            }

        duration = (datetime.utcnow() - start_time).total_seconds()
        return {
            "status": "success",
            "table": result["table"],
            "rows_processed": result["rows_processed"],
            "batches": result["batches"],
            "child_tables": result["child_tables"],
            "pagination": result["pagination"],
            "pk_columns": result["pk_columns"],
            "filter_column": params["filter_column"],
            "filter_values": params["filter_values"],
            "batch_size": params["batch_size"],
            "duration_seconds": round(duration, 2),
        }

    except Exception as e:
        _LOG.error(f"Activity failed: {str(e)}", exc_info=True)
        return {"status": "error", "message": str(e)}

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================================
# ORCHESTRATOR AND HTTP TRIGGER
# ============================================================================


@app.orchestration_trigger(context_name="context")
def postgres_to_adls_orchestrator(context: df.DurableOrchestrationContext):
    """
    Durable Functions orchestrator that delegates to the activity function.

    Reads the input payload from the orchestration context and invokes the
    process_postgres_to_adls_activity activity, yielding control until it
    completes. Returns the activity result directly to the caller.
    """
    params = context.get_input()
    result = yield context.call_activity("process_postgres_to_adls_activity", params)
    return result


@app.route(route="Postgres_to_ADLS_V1", methods=["POST"])
@app.durable_client_input(client_name="client")
async def postgres_to_adls_http_start(
    req: func.HttpRequest, client
) -> func.HttpResponse:
    """
    HTTP trigger that starts a new orchestration instance for the PostgreSQL-to-ADLS pipeline.

    Parses the JSON request body and passes it as input to the
    postgres_to_adls_orchestrator orchestration. Returns a 400 response on
    invalid JSON, the Durable Functions check-status response on success, or a
    500 JSON error response if starting the orchestration fails.
    """
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse("Invalid JSON body", status_code=400)
    try:
        instance_id = await client.start_new(
            "postgres_to_adls_orchestrator", None, body
        )
        return client.create_check_status_response(req, instance_id)
    except Exception as e:
        _LOG.error(f"HTTP start failed: {str(e)}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
