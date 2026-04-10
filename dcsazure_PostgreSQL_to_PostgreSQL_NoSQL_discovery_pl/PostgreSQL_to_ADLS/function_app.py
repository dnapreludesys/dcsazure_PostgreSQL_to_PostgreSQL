import gc
import json
import logging
import uuid
import xml.etree.ElementTree as ET
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

# Default batch size for processing rows
DEFAULT_BATCH_SIZE = 500

# Limits how many child rows are processed at once to avoid high memory usage.
ARRAY_PROCESSING_BATCH_SIZE = 2000

# File Handling Constants
PIPE_DELIMITER = "|"
ESCAPE_CHARACTER = "\\"

# PostgreSQL column type categories
JSON_TYPES = {"json", "jsonb"}
XML_TYPES = {"xml"}

# ============================================================================
# AZURE FUNCTIONS APP
# ============================================================================

# FIX 1: must be df.DFApp (not func.FunctionApp) to support durable triggers
app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


# ============================================================================
# KEY VAULT UTILITIES
# ============================================================================


def get_secret(key_vault_name: str, secret_name: str) -> str:
    """
    Retrieve a secret from Azure Key Vault.

    Args:
        key_vault_name: Name of the Key Vault
        secret_name: Name of the secret to retrieve

    Returns:
        The secret value as a string
    """
    kv_uri = f"https://{key_vault_name}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=kv_uri, credential=credential)
    secret = client.get_secret(secret_name)
    return secret.value


# ============================================================================
# POSTGRESQL CONNECTION & METADATA UTILITIES
# ============================================================================


def get_postgres_connection(
    pg_host: str,
    pg_port: int,
    pg_database: str,
    pg_username: str,
    pg_password: str,
    pg_sslmode: str = "require",
):
    """
    Create and return a psycopg2 connection to PostgreSQL.

    Args:
        pg_host: PostgreSQL host
        pg_port: PostgreSQL port
        pg_database: Database name
        pg_username: Username
        pg_password: Password
        pg_sslmode: SSL mode (default: 'require' — appropriate for GCP Cloud SQL)

    Returns:
        psycopg2 connection object

    Raises:
        ValueError: If connection fails
    """
    try:
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            dbname=pg_database,
            user=pg_username,
            password=pg_password,
            sslmode=pg_sslmode,
            connect_timeout=30,
        )
        return conn
    except psycopg2.OperationalError as e:
        raise ValueError(f"PostgreSQL connection failed: {str(e)}")
    except Exception as e:
        raise ValueError(f"Unexpected error connecting to PostgreSQL: {str(e)}")


def get_table_primary_keys(conn, schema: str, table: str) -> List[str]:
    """
    Discover primary key column(s) for a given table.

    Args:
        conn: psycopg2 connection
        schema: Schema name
        table: Table name

    Returns:
        Ordered list of primary key column names
    """
    query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = %s
          AND tc.table_name = %s
        ORDER BY kcu.ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        rows = cur.fetchall()
    return [row[0] for row in rows]


def get_table_column_types(conn, schema: str, table: str) -> Dict[str, str]:
    """
    Retrieve column names and their data types for a given table.

    Args:
        conn: psycopg2 connection
        schema: Schema name
        table: Table name

    Returns:
        Dict mapping column_name -> data_type (lowercased)
    """
    query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        rows = cur.fetchall()
    return {row[0]: row[1].lower() for row in rows}


def validate_table_exists(conn, schema: str, table: str) -> None:
    """
    Validate that the requested table exists in the given schema.

    Args:
        conn: psycopg2 connection
        schema: Schema name
        table: Table name to validate

    Raises:
        ValueError: If the table does not exist
    """
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
          AND table_type IN ('BASE TABLE', 'VIEW')
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        exists = cur.fetchone()

    if not exists:
        raise ValueError(
            f"Table '{table}' does not exist in schema '{schema}'."
        )


# ============================================================================
# STREAMING POSTGRESQL READER
# ============================================================================


class StreamingPostgresReader:
    """
    Stream rows from a PostgreSQL table in batches using keyset pagination.

    Uses primary key column(s) for efficient cursor-based pagination.
    Falls back to LIMIT/OFFSET if no primary key is available.
    Minimises memory usage by yielding one batch at a time.
    Optionally filters rows using an IN clause on a scalar column.
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
        Initialise the streaming reader.

        Args:
            conn: psycopg2 connection
            schema: Schema name
            table: Table name
            pk_columns: Primary key column(s) used for keyset pagination
            batch_size: Number of rows per batch
            column_types: Dict of column_name -> data_type
            filter_column: Optional scalar column to filter on
            filter_values: Optional list of values for the IN clause
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
        Stream rows from the table in batches.

        Yields:
            Lists of row dicts (column_name -> value), one batch at a time
        """
        if self._use_keyset:
            yield from self._stream_keyset()
        else:
            yield from self._stream_offset()

    def _build_filter_clause(self) -> Tuple[str, list]:
        """
        Build the SQL IN filter clause and its parameter values.

        Returns:
            Tuple of (sql_fragment, params_list).
            sql_fragment is empty string if no filter is set.
        """
        if not self.filter_column or not self.filter_values:
            return "", []
        quoted_col = f'"{self.filter_column}"'
        placeholders = ", ".join(["%s"] * len(self.filter_values))
        return f"{quoted_col} IN ({placeholders})", list(self.filter_values)

    def _stream_keyset(self) -> Iterator[List[Dict]]:
        """
        Keyset (cursor-based) pagination using the primary key column(s).

        Constructs a WHERE clause like:
            WHERE filter_col IN (%s, %s) AND (pk1, pk2) > (%s, %s)
        to page through rows without skipping or duplicating.
        The filter clause is omitted when no filter is configured.

        Yields:
            Batches of row dicts
        """
        quoted_schema = f'"{self.schema}"'
        quoted_table = f'"{self.table}"'
        quoted_pks = [f'"{c}"' for c in self.pk_columns]

        order_clause = ", ".join(quoted_pks)

        pk_placeholders = ", ".join(["%s"] * len(self.pk_columns))
        pk_tuple = f"({', '.join(quoted_pks)})"
        keyset_clause = f"{pk_tuple} > ({pk_placeholders})"

        filter_sql, filter_params = self._build_filter_clause()

        # Base query (first batch — no keyset anchor yet)
        if filter_sql:
            base_query = (
                f"SELECT * FROM {quoted_schema}.{quoted_table} "
                f"WHERE {filter_sql} "
                f"ORDER BY {order_clause} "
                f"LIMIT %s"
            )
        else:
            base_query = (
                f"SELECT * FROM {quoted_schema}.{quoted_table} "
                f"ORDER BY {order_clause} "
                f"LIMIT %s"
            )

        # Keyset query (subsequent batches — anchored on last PK values)
        if filter_sql:
            keyset_query = (
                f"SELECT * FROM {quoted_schema}.{quoted_table} "
                f"WHERE {filter_sql} AND {keyset_clause} "
                f"ORDER BY {order_clause} "
                f"LIMIT %s"
            )
        else:
            keyset_query = (
                f"SELECT * FROM {quoted_schema}.{quoted_table} "
                f"WHERE {keyset_clause} "
                f"ORDER BY {order_clause} "
                f"LIMIT %s"
            )

        last_pk_values = None

        while True:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if last_pk_values is None:
                    cur.execute(base_query, (*filter_params, self.batch_size))
                else:
                    cur.execute(keyset_query, (*filter_params, *last_pk_values, self.batch_size))

                rows = cur.fetchall()

            if not rows:
                break

            batch = [dict(row) for row in rows]
            self.batches_fetched += 1
            self.rows_fetched += len(batch)

            yield batch

            if len(batch) < self.batch_size:
                break

            last_pk_values = tuple(batch[-1][pk] for pk in self.pk_columns)
            gc.collect()

    def _stream_offset(self) -> Iterator[List[Dict]]:
        """
        Fallback LIMIT/OFFSET pagination when no primary key is available.

        Yields:
            Batches of row dicts
        """
        quoted_schema = f'"{self.schema}"'
        quoted_table = f'"{self.table}"'
        offset = 0

        filter_sql, filter_params = self._build_filter_clause()

        if filter_sql:
            query = (
                f"SELECT * FROM {quoted_schema}.{quoted_table} "
                f"WHERE {filter_sql} "
                f"LIMIT %s OFFSET %s"
            )
        else:
            query = (
                f"SELECT * FROM {quoted_schema}.{quoted_table} "
                f"LIMIT %s OFFSET %s"
            )

        while True:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (*filter_params, self.batch_size, offset))
                rows = cur.fetchall()

            if not rows:
                break

            batch = [dict(row) for row in rows]
            self.batches_fetched += 1
            self.rows_fetched += len(batch)

            yield batch

            if len(batch) < self.batch_size:
                break

            offset += len(batch)
            gc.collect()

    def get_stats(self) -> Dict:
        """Return reader statistics."""
        return {
            "batches_fetched": self.batches_fetched,
            "rows_fetched": self.rows_fetched,
            "pagination_strategy": "keyset" if self._use_keyset else "offset",
        }


# ============================================================================
# JSON / XML PARSING UTILITIES
# ============================================================================


def parse_json_value(value) -> Optional[Dict]:
    """
    Parse a JSON/JSONB column value into a Python dict.

    psycopg2 already deserialises JSONB to dicts; this handles
    the case where the value arrives as a raw string.

    Args:
        value: Raw column value

    Returns:
        Parsed dict, or None if unparseable
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None
    return None


def parse_xml_value(value) -> Optional[Dict]:
    """
    Parse an XML column value into a nested Python dict that mirrors the
    structure of a parsed JSON object.

    The result is intentionally identical in shape to what parse_json_value
    returns so that _extract_arrays_from_dict can process XML and JSON
    columns through exactly the same code path.

    Rules:
    - The root element tag is NOT included in any key — its children
      form the top-level keys directly (the PG column name already
      identifies the root).
    - Scalar element text → plain string value.
    - Element attributes → stored as "<tag>@<attr>" key on the parent dict.
    - Single child element → nested dict under its tag key.
    - Multiple sibling elements with the same tag → list of dicts (or list
      of strings if all are text-only), matching how JSON arrays look.
    - Primitive (text-only, no sub-elements, no attributes) repeated
      siblings → list of strings, kept as a scalar column in the parent
      (same as a primitive JSON array).
    - Complex repeated siblings → list of dicts, which _extract_arrays_from_dict
      will detect and split into a child table with _has_array_ flag.

    Example:
        <order>
            <id>1</id>
            <amount>50</amount>
            <tags><tag>a</tag><tag>b</tag></tags>
            <items>
                <item><name>x</name></item>
                <item><name>y</name></item>
            </items>
        </order>
    Returns:
        {
            "id": "1",
            "amount": "50",
            "tags.tag": ["a", "b"],        # primitive list → scalar column
            "items.item": [                 # complex list → child table
                {"name": "x"},
                {"name": "y"}
            ]
        }

    Args:
        value: Raw XML string

    Returns:
        Nested dict matching parsed JSON shape, or None if unparseable
    """
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    try:
        root = ET.fromstring(value.strip())
        # Pass prefix="" so root tag never appears in any key
        return _xml_element_to_dict(root)
    except ET.ParseError:
        return None


def _xml_element_to_dict(element: ET.Element) -> Dict:
    """
    Convert an XML element and all its descendants into a nested Python
    dict that mirrors parsed JSON structure.

    This produces the same shape _extract_arrays_from_dict expects:
    - Scalar values are plain strings/numbers.
    - Single nested elements are dicts.
    - Repeated sibling elements are lists of dicts (complex) or strings
      (text-only), matching JSON array semantics exactly.

    Args:
        element: XML element to convert

    Returns:
        Nested dict
    """
    result = {}

    # Attributes stored as tag@attr_name on the current dict
    for attr_name, attr_value in element.attrib.items():
        result[f"@{attr_name}"] = attr_value

    # Group children by tag to detect repetition
    children_by_tag: Dict[str, List[ET.Element]] = {}
    for child in element:
        children_by_tag.setdefault(child.tag, []).append(child)

    for tag, children in children_by_tag.items():
        all_text_only = all(
            len(child) == 0 and not child.attrib
            for child in children
        )

        if len(children) == 1:
            child = children[0]
            if len(child) == 0 and not child.attrib:
                # Leaf element — scalar value
                result[tag] = (child.text or "").strip()
            else:
                # Single nested element — recurse into dict
                result[tag] = _xml_element_to_dict(child)
        else:
            if all_text_only:
                # Repeated text-only siblings → primitive list (stays as column)
                result[tag] = [(child.text or "").strip() for child in children]
            else:
                # Repeated complex siblings → list of dicts (becomes child table)
                result[tag] = [_xml_element_to_dict(child) for child in children]

    # Text content of this element (only meaningful for mixed-content elements)
    text = (element.text or "").strip()
    if text and not children_by_tag:
        # Pure text node with no children — return text as the value
        # This is handled by the caller (single leaf case above)
        # For the root call context this sets the element value directly
        result["_text"] = text

    return result


# ============================================================================
# STRUCTURED COLUMN EXTRACTION
# ============================================================================


def extract_structured_columns(
    row: Dict,
    column_types: Dict[str, str],
    row_rid: str,
) -> Tuple[Dict, Dict[str, List[Dict]]]:
    """
    Process a single PostgreSQL row, extracting JSON/XML columns into
    child records while keeping scalar columns in the parent.

    For each JSON/JSONB or XML column:
      - A flag `has_json_<col>` or `has_xml_<col>` is added to the parent row.
      - The raw column value is REMOVED from the parent row (replaced by the flag).
      - The parsed content is placed into a child table named `<col>`.
      - If the parsed content contains arrays of objects, those are further
        extracted into nested child tables using the same _rid/_parent_rid chain.

    Args:
        row: Raw row dict from PostgreSQL
        column_types: Dict of column_name -> pg data_type
        row_rid: The _rid assigned to this parent row

    Returns:
        Tuple of:
            - parent_fields: Scalar fields for the parent CSV (with flags)
            - child_records: Dict of child_table_name -> list of child row dicts
    """
    parent_fields = {}
    child_records: Dict[str, List[Dict]] = {}

    for col, value in row.items():
        col_type = column_types.get(col, "")

        # ── JSON / JSONB ──────────────────────────────────────────────────
        if col_type in JSON_TYPES:
            parsed = parse_json_value(value)
            if parsed is not None:
                parent_fields[f"has_json_{col}"] = True
                child_parent_fields, nested_children = _extract_arrays_from_dict(
                    parsed, parent_rid=row_rid, table_prefix=col
                )
                child_row = {"_rid": str(uuid.uuid4()), "_parent_rid": row_rid}
                child_row.update(child_parent_fields)
                child_records.setdefault(col, []).append(child_row)

                for nested_name, nested_rows in nested_children.items():
                    child_records.setdefault(nested_name, []).extend(nested_rows)
            else:
                parent_fields[f"has_json_{col}"] = False
                parent_fields[col] = value

        # ── XML ───────────────────────────────────────────────────────────
        # Handled identically to JSON: parse_xml_value now returns a nested
        # dict in the same shape as parse_json_value, so _extract_arrays_from_dict
        # processes both through the exact same BFS queue algorithm.
        elif col_type in XML_TYPES:
            parsed = parse_xml_value(value)
            if parsed is not None:
                parent_fields[f"has_xml_{col}"] = True
                child_parent_fields, nested_children = _extract_arrays_from_dict(
                    parsed, parent_rid=row_rid, table_prefix=col
                )
                child_row = {"_rid": str(uuid.uuid4()), "_parent_rid": row_rid}
                child_row.update(child_parent_fields)
                child_records.setdefault(col, []).append(child_row)

                for nested_name, nested_rows in nested_children.items():
                    child_records.setdefault(nested_name, []).extend(nested_rows)
            else:
                parent_fields[f"has_xml_{col}"] = False
                parent_fields[col] = value

        # ── Scalar / other ────────────────────────────────────────────────
        else:
            parent_fields[col] = value

    return parent_fields, child_records


def _extract_arrays_from_dict(
    doc: Dict,
    parent_rid: str,
    table_prefix: str,
) -> Tuple[Dict, Dict[str, List[Dict]]]:
    """
    Extract nested arrays from a parsed JSON dict (same algorithm as
    extract_arrays_iterative_optimized in the Cosmos version).

    Scalar and primitive-array fields stay in the returned parent_fields dict.
    Arrays of objects are extracted into child tables keyed by
    '<table_prefix>.<array_key>' (and further nested as needed).

    Args:
        doc: Parsed JSON dict
        parent_rid: _rid of the immediate parent row
        table_prefix: Dot-notation prefix for naming child tables

    Returns:
        Tuple of:
            - parent_fields: Flattened scalar fields
            - child_records: Dict of table_name -> list of row dicts
    """
    parent_fields: Dict = {}
    child_records: Dict[str, List[Dict]] = {}

    queue = deque()

    def flatten_no_arrays(d: Dict, parent: str = "") -> Dict:
        flat = {}
        for k, v in d.items():
            key = f"{parent}.{k}" if parent else k
            if isinstance(v, list):
                flat[key] = v
            elif isinstance(v, dict):
                flat.update(flatten_no_arrays(v, key))
            else:
                flat[key] = v
        return flat

    flat_root = flatten_no_arrays(doc)

    for key, value in flat_root.items():
        if not isinstance(value, list):
            parent_fields[key] = value
            continue

        primitive_items = [v for v in value if not isinstance(v, dict)]
        dict_items = [v for v in value if isinstance(v, dict)]

        if primitive_items and not dict_items:
            parent_fields[key] = value
            continue

        parent_fields[f"_has_array_{key}"] = True
        for item in dict_items:
            item_rid = str(uuid.uuid4())
            item["_rid"] = item_rid
            item["_parent_rid"] = parent_rid
            queue.append((item, f"{table_prefix}.{key}", parent_rid))

    current_batch: Dict[str, List[Dict]] = {}

    while queue:
        current, tbl_name, par_rid = queue.popleft()
        flat = flatten_no_arrays(current)
        row = {"_rid": current["_rid"], "_parent_rid": par_rid}

        for key, value in flat.items():
            if not isinstance(value, list):
                row[key] = value
                continue

            primitive_items = [v for v in value if not isinstance(v, dict)]
            dict_items = [v for v in value if isinstance(v, dict)]

            if primitive_items and not dict_items:
                row[key] = value
                continue

            row[f"_has_array_{key}"] = True
            for item in dict_items:
                item_rid = str(uuid.uuid4())
                item["_rid"] = item_rid
                item["_parent_rid"] = current["_rid"]
                queue.append((item, f"{tbl_name}.{key}", current["_rid"]))

        current_batch.setdefault(tbl_name, []).append(row)

        if len(current_batch.get(tbl_name, [])) >= ARRAY_PROCESSING_BATCH_SIZE:
            child_records.setdefault(tbl_name, []).extend(current_batch[tbl_name])
            current_batch[tbl_name] = []

    for tbl_name, rows in current_batch.items():
        if rows:
            child_records.setdefault(tbl_name, []).extend(rows)

    return parent_fields, child_records


# ============================================================================
# DATA CLEANING
# ============================================================================


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean a DataFrame by converting unparseable values to None.

    Handles:
    - Nested dicts -> None
    - Arrays containing dicts -> None
    - Simple values (str, int, float, bool) -> preserved
    - Simple arrays (no dicts) -> preserved

    Args:
        df: DataFrame to clean

    Returns:
        Cleaned DataFrame
    """
    if df is None or df.empty:
        return pd.DataFrame()

    def fix_value(val):
        if val is None:
            return None
        if isinstance(val, (str, int, float, bool)):
            return val
        if isinstance(val, list) and all(not isinstance(i, dict) for i in val):
            return val
        if isinstance(val, dict):
            return None
        if isinstance(val, list) and any(isinstance(i, dict) for i in val):
            return None
        try:
            return str(val)
        except Exception:
            return None

    for col in df.columns:
        df[col] = df[col].apply(fix_value)

    return df


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
    Upload a DataFrame to Azure Data Lake Storage as a pipe-delimited CSV.

    Supports write and append modes with automatic schema evolution
    (new columns detected in later batches are back-filled into the existing file).

    Args:
        service_client: ADLS DataLakeServiceClient
        file_system: ADLS container/filesystem name
        directory: Target directory path inside the filesystem
        file_name: Name for the CSV file
        df: DataFrame to upload
        mode: 'write' (overwrite) or 'append'
        known_columns: Set of previously seen column names for schema tracking

    Returns:
        Updated set of all known columns after this upload
    """
    df = clean_dataframe(df)
    if df is None or df.empty:
        return known_columns or set()

    fs_client = service_client.get_file_system_client(file_system)

    # Create directory hierarchy
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

    # ── Schema evolution: new columns appeared in this batch ─────────────
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
                file_client.append_data(content_bytes, offset=0, length=len(content_bytes))
                file_client.flush_data(len(content_bytes))
                return known_columns
        except Exception:
            pass

    # ── Standard write / append ───────────────────────────────────────────
    for col in known_columns:
        if col not in df.columns:
            df[col] = None

    if len(known_columns) > 0:
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
            # Append mode
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
        logging.error(f"Upload failed for {file_name}: {str(e)}")
        raise

    known_columns = known_columns | current_columns
    return known_columns


# ============================================================================
# ADLS CONNECTION VALIDATION
# ============================================================================


def validate_adls_connection(
    adls_account_name: str,
    adls_file_system: str,
    adls_account_key: str,
):
    """
    Validate ADLS Gen2 connection using a storage account key and return
    the service client.

    Args:
        adls_account_name: Storage account name
        adls_file_system: Container/filesystem name
        adls_account_key: Storage account key (retrieved from Key Vault)

    Returns:
        DataLakeServiceClient

    Raises:
        ValueError: If connection or filesystem validation fails
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
    Validate and extract all required parameters from the input dict.

    Required parameters:
        pg_host, pg_port, pg_database, pg_username,
        pg_secret_name, key_vault_name,
        table (single table name string),
        adls_account_name, adls_file_system, adls_secret_name

    Optional parameters:
        pg_schema (default: 'public')
        pg_sslmode (default: 'require')
        adls_directory (default: '')
        batch_size (default: DEFAULT_BATCH_SIZE)
        filter_column - scalar column name to filter on
        filter_value  - list of values for the IN clause (requires filter_column)

    Args:
        params: Raw input parameters dict

    Returns:
        Validated and normalised parameters dict

    Raises:
        ValueError: If required parameters are missing or invalid
    """
    pg_host = params.get("pg_host")
    pg_port = params.get("pg_port", 5432)
    pg_database = params.get("pg_database")
    pg_username = params.get("pg_username")
    pg_secret_name = params.get("pg_secret_name")
    key_vault_name = params.get("key_vault_name")
    table = params.get("table")
    adls_account_name = params.get("adls_account_name")
    adls_file_system = params.get("adls_file_system")
    adls_secret_name = params.get("adls_secret_name")

    missing = [
        k
        for k, v in {
            "pg_host": pg_host,
            "pg_database": pg_database,
            "pg_username": pg_username,
            "pg_secret_name": pg_secret_name,
            "key_vault_name": key_vault_name,
            "table": table,
            "adls_account_name": adls_account_name,
            "adls_file_system": adls_file_system,
            "adls_secret_name": adls_secret_name,
        }.items()
        if not v
    ]
    if missing:
        raise ValueError(f"Missing required parameters: {missing}")

    # Validate table is a plain string — no commas, no arrays
    if not isinstance(table, str) or not table.strip():
        raise ValueError("'table' must be a non-empty string. Only one table per invocation.")
    table = table.strip()

    # ── Optional filter params ─────────────────────────────────────────────
    filter_column = params.get("filter_column")
    filter_value = params.get("filter_value")

    has_filter_col = bool(filter_column and str(filter_column).strip())
    has_filter_val = bool(filter_value is not None and filter_value != [])

    if has_filter_col != has_filter_val:
        raise ValueError(
            "Invalid filter configuration: "
            "Both 'filter_column' AND 'filter_value' must be provided together."
        )

    if has_filter_col:
        filter_column = str(filter_column).strip()
        if isinstance(filter_value, list):
            filter_values = [v for v in filter_value if v is not None]
        else:
            filter_values = [filter_value]
        if not filter_values:
            raise ValueError("'filter_value' was provided but is empty.")
    else:
        filter_column = None
        filter_values = None

    try:
        pg_port = int(pg_port)
    except (ValueError, TypeError):
        pg_port = 5432

    batch_size = params.get("batch_size", DEFAULT_BATCH_SIZE)
    try:
        batch_size = int(batch_size)
        if batch_size < 1:
            batch_size = DEFAULT_BATCH_SIZE
    except (ValueError, TypeError):
        batch_size = DEFAULT_BATCH_SIZE

    return {
        "pg_host": pg_host,
        "pg_port": pg_port,
        "pg_database": pg_database,
        "pg_username": pg_username,
        "pg_secret_name": pg_secret_name,
        "key_vault_name": key_vault_name,
        "pg_schema": params.get("pg_schema", "public"),
        "pg_sslmode": params.get("pg_sslmode", "require"),
        "table": table,
        "filter_column": filter_column,
        "filter_values": filter_values,
        "adls_account_name": adls_account_name,
        "adls_file_system": adls_file_system,
        "adls_secret_name": adls_secret_name,
        "adls_directory": params.get("adls_directory", ""),
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
    Export a single PostgreSQL table to ADLS.

    Steps:
      1. Introspect column types and primary keys.
      2. Validate filter_column is a scalar (non JSON/XML) column if provided.
      3. Stream rows in batches via keyset (or offset) pagination,
         applying the IN filter if configured.
      4. For each batch, separate scalar columns (parent) from
         JSON/XML columns (child tables).
      5. Add _rid to every parent row; link child rows via _parent_rid.
      6. Upload parent and all child tables to ADLS as pipe-delimited CSVs.

    Args:
        conn: psycopg2 connection
        service_client: ADLS DataLakeServiceClient
        schema: PostgreSQL schema name
        table: Table name
        batch_size: Rows per batch
        adls_file_system: ADLS filesystem name
        export_dir: Base ADLS directory for this export run
        filter_column: Optional scalar column name to filter on
        filter_values: Optional list of values for the IN clause

    Returns:
        Stats dict for this table
    """
    column_types = get_table_column_types(conn, schema, table)
    pk_columns = get_table_primary_keys(conn, schema, table)

    if not pk_columns:
        logging.warning(
            f"Table '{schema}.{table}' has no primary key. "
            "Falling back to LIMIT/OFFSET pagination."
        )

    # ── Validate filter_column is scalar (not JSON/XML) ───────────────────
    if filter_column:
        if filter_column not in column_types:
            raise ValueError(
                f"filter_column '{filter_column}' does not exist in table '{table}'."
            )
        if column_types[filter_column] in JSON_TYPES | XML_TYPES:
            raise ValueError(
                f"filter_column '{filter_column}' is a {column_types[filter_column]} column. "
                "Filtering is only supported on scalar (non JSON/XML) columns."
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

    # ADLS paths
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

        # ── Upload parent ─────────────────────────────────────────────────
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

        # ── Upload child tables ───────────────────────────────────────────
        for child_name, child_rows in batch_child_records.items():
            child_table_names.add(child_name)
            child_df = pd.DataFrame(child_rows)
            child_df = clean_dataframe(child_df)

            if child_df is None or child_df.empty:
                continue

            # Derive directory and filename from dot-notation child name
            # e.g. "metadata.tags" -> dir: <table>/metadata, file: tags.csv
            parts = child_name.split(".")
            child_dir = f"{table_dir}/{'/'.join(parts)}"
            child_file = f"{parts[-1]}.csv"

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

    reader_stats = reader.get_stats()

    return {
        "table": table,
        "rows_processed": rows_processed,
        "batches": batch_num,
        "child_tables": list(child_table_names),
        "pagination": reader_stats["pagination_strategy"],
        "pk_columns": pk_columns,
    }


# ============================================================================
# MAIN ACTIVITY FUNCTION
# ============================================================================


@app.activity_trigger(input_name="params")
def process_postgres_to_adls_activity(params: dict):
    """
    Azure Durable Function activity to export PostgreSQL tables to ADLS Gen2.

    This function:
      1. Validates and extracts all input parameters.
      2. Retrieves the PostgreSQL password from Azure Key Vault.
      3. Connects to the PostgreSQL database.
      4. Validates that all requested tables exist.
      5. Validates the ADLS connection via Managed Identity.
      6. For each table, streams rows in batches, separates scalar /
         JSON / XML columns, and uploads CSVs to ADLS.

    Input parameters (JSON body):
      Required:
        pg_host           - PostgreSQL hostname (GCP Cloud SQL public/private IP or DNS)
        pg_database       - Database name
        pg_username       - Database username (plain text)
        pg_secret_name    - Key Vault secret name holding the DB password
        key_vault_name    - Azure Key Vault name
        table             - Single table name (string)
        adls_account_name - Azure Storage account name
        adls_file_system  - ADLS Gen2 container/filesystem name

      Optional:
        pg_port           - PostgreSQL port (default: 5432)
        pg_schema         - PostgreSQL schema (default: 'public')
        pg_sslmode        - SSL mode (default: 'require')
        adls_directory    - Base directory prefix in ADLS (default: '')
        batch_size        - Rows per batch (default: 500)

      Optional:
        filter_column     - Scalar column name to filter on
        filter_value      - Array of values; only rows matching these values are exported
                            (both filter_column and filter_value must be provided together)

    Output CSV layout in ADLS:
      <adls_directory>/<table>/
          <table>.csv                  <- parent rows; scalar columns + _rid + has_json_*/has_xml_* flags
          <json_col>/
              <json_col>.csv           <- top-level JSON fields; _rid + _parent_rid
              <array_key>/
                  <array_key>.csv      <- nested array rows; _rid + _parent_rid
          <xml_col>/
              <xml_col>.csv            <- flattened XML fields; _rid + _parent_rid
              <repeated_tag>/
                  <repeated_tag>.csv   <- repeated XML elements; _rid + _parent_rid

    Rejoining:
      JOIN child ON child._parent_rid = parent._rid

    Args:
        params: Activity input dict (see above)

    Returns:
        Dict with status and per-table statistics
    """
    start_time = datetime.utcnow()
    conn = None

    try:
        params = validate_and_extract_params(params)

        # Retrieve password from Key Vault
        pg_password = get_secret(
            key_vault_name=params["key_vault_name"],
            secret_name=params["pg_secret_name"],
        )

        # Connect to PostgreSQL
        conn = get_postgres_connection(
            pg_host=params["pg_host"],
            pg_port=params["pg_port"],
            pg_database=params["pg_database"],
            pg_username=params["pg_username"],
            pg_password=pg_password,
            pg_sslmode=params["pg_sslmode"],
        )

        # Validate table exists
        validate_table_exists(conn, params["pg_schema"], params["table"])

        # Retrieve ADLS account key from Key Vault
        adls_account_key = get_secret(
            key_vault_name=params["key_vault_name"],
            secret_name=params["adls_secret_name"],
        )

        # Connect to ADLS
        service_client = validate_adls_connection(
            params["adls_account_name"],
            params["adls_file_system"],
            adls_account_key,
        )

        export_dir = (
            f"{params['adls_directory']}/{params['pg_database']}/{params['pg_schema']}"
            if params["adls_directory"]
            else f"{params['pg_database']}/{params['pg_schema']}"
        )

        table = params["table"]
        logging.info(f"Processing table: {params['pg_schema']}.{table}")

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
        logging.error(f"Activity failed: {str(e)}", exc_info=True)
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
    Durable orchestrator for the PostgreSQL to ADLS export process.

    Args:
        context: Durable orchestration context

    Returns:
        Result from the activity function
    """
    params = context.get_input()
    result = yield context.call_activity("process_postgres_to_adls_activity", params)
    return result


# FIX 2: replaced placeholder function body with real orchestration trigger implementation
@app.route(route="Postgres_to_ADLS_V1", methods=["POST"])
@app.durable_client_input(client_name="client")
async def postgres_to_adls_http_start(
    req: func.HttpRequest, client
) -> func.HttpResponse:
    """
    HTTP trigger to start the PostgreSQL to ADLS export orchestration.

    Args:
        req: HTTP request with JSON body (see activity docstring for parameters)
        client: Durable orchestration client

    Returns:
        HTTP response with orchestration status check URLs
    """
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    try:
        instance_id = await client.start_new(
            "postgres_to_adls_orchestrator", None, body
        )
        response = client.create_check_status_response(req, instance_id)
        return response
    except Exception as e:
        logging.error(f"HTTP start failed: {str(e)}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )