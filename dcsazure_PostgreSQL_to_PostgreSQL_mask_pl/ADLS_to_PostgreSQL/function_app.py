import ast
import csv
import gc
import io
import json
import logging
import re
import time
import defusedxml.ElementTree as ET
from xml.etree.ElementTree import Element, SubElement
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

import azure.durable_functions as df
import azure.functions as func
import pandas as pd
import psycopg2
from azure.core.exceptions import ClientAuthenticationError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.filedatalake import DataLakeServiceClient

# ============================================================================
# CONSTANTS
# ============================================================================

DEFAULT_BATCH_SIZE = 10_000
PARENT_STREAM_CHUNK_SIZE = 10_000
CHILD_CHUNK_SIZE = 10_000
PROGRESS_LOG_INTERVAL = 5

PIPE_DELIMITER = "|"
ESCAPE_CHARACTER = "\\"

JSON_TYPES = {"json", "jsonb"}
XML_TYPES = {"xml"}
BOOL_TYPES = {"boolean"}
INT_TYPES = {"smallint", "integer", "bigint", "int2", "int4", "int8"}
FLOAT_TYPES = {"real", "double precision", "float4", "float8"}
NUMERIC_TYPES = {"numeric", "decimal"}
UUID_TYPES = {"uuid"}
DATE_TYPES = {"date"}
TIMESTAMP_TYPES = {
    "timestamp without time zone",
    "timestamp with time zone",
    "timestamp",
}
TIME_TYPES = {
    "time without time zone",
    "time with time zone",
    "time",
}
INTERVAL_TYPES = {"interval"}
BYTES_TYPES = {"bytea"}
ARRAY_TYPES = {"array"}
TEXT_TYPES = {
    "text",
    "character varying",
    "varchar",
    "character",
    "char",
    "bpchar",
    "name",
}

PD_NULL_STRINGS = frozenset({"nan", "nat", "none", "<na>", "null", ""})

PG_RETRY_MAX = 5
PG_RETRY_BASE_SEC = 1.0

PREVIEW_MAX_LENGTH = 120
COPY_BUFFER_DEBUG_ROWS = 3
ERROR_SAMPLE_LIMIT = 10
BATCH_ERROR_SAMPLE_LIMIT = 3
SUSPECT_JSON_PREVIEW_LENGTH = 200
COPY_ROW_PREVIEW_LENGTH = 500

DEFAULT_PG_PORT = 5432
DEFAULT_PG_SCHEMA = "public"
DEFAULT_PG_SSLMODE = "require"
DEFAULT_ADLS_DIRECTORY = ""
DEFAULT_CONNECT_TIMEOUT = 30

_LOG = logging.getLogger(__name__)


# ============================================================================
# AZURE FUNCTIONS APP
# ============================================================================

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def _preview(value: Any, max_length: int = PREVIEW_MAX_LENGTH) -> str:
    """
    Return a truncated string preview of any value.

    Converts the value to a string using str(), falling back to repr() on
    failure, then clips to max_length characters and appends an ellipsis
    if the original string exceeded that length.
    """
    try:
        s = str(value)
    except Exception:
        s = repr(value)
    return s[:max_length] + ("…" if len(s) > max_length else "")


def _is_suspect_json(s: str) -> bool:
    """
    Return True if the string looks like JSON but fails to parse.

    A value is considered suspect when it starts with '{' or '[' (suggesting
    JSON intent) but json.loads raises a JSONDecodeError, indicating malformed
    or incomplete JSON that could cause downstream database errors.
    """
    stripped = s.strip() if isinstance(s, str) else ""
    if not stripped or stripped[0] not in ("{", "["):
        return False
    try:
        json.loads(stripped)
        return False
    except json.JSONDecodeError:
        return True


def _sanitise_copy_cell(value: str) -> str:
    """
    Replace embedded newlines and carriage-returns in a cell value with a space.

    PostgreSQL COPY commands interpret bare newline and carriage-return characters
    as row terminators, so any such characters inside a field value must be
    normalised before the buffer is written to the COPY stream.
    """
    return value.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")


# ============================================================================
# KEY VAULT
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
# ADLS HELPERS
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
        list(
            service_client.get_file_system_client(adls_file_system).get_paths(
                path="", max_results=1
            )
        )
        return service_client
    except ClientAuthenticationError:
        raise ValueError("ADLS authentication failed. Check Key Vault secret.")
    except ResourceNotFoundError:
        raise ValueError(f"ADLS filesystem '{adls_file_system}' does not exist.")
    except Exception as e:
        raise ValueError(f"ADLS connection validation failed: {str(e)}")


def stream_parent_csv_in_chunks(
    service_client: DataLakeServiceClient,
    file_system: str,
    full_path: str,
    chunk_rows: int = PARENT_STREAM_CHUNK_SIZE,
) -> Iterator[pd.DataFrame]:
    """
    Stream a pipe-delimited parent CSV file from ADLS in fixed-size row chunks.

    Opens the specified file using the DataLake file client, wraps the raw
    download stream in an _AdlsRawStream adapter, and feeds it to pandas
    read_csv with chunked iteration. Each yielded DataFrame contains at most
    chunk_rows rows. Memory is explicitly freed after each chunk via del and
    gc.collect(). Quotes are disabled (quoting=0), backslash is the escape
    character, and empty strings are preserved rather than converted to NaN.

    chunk_rows is driven by the caller-supplied batch_size parameter so that
    the parent read window matches every other stage in the pipeline.
    """
    fs_client = service_client.get_file_system_client(file_system)
    file_client = fs_client.get_file_client(full_path)
    downloader = file_client.download_file()
    raw_stream = _AdlsRawStream(downloader)

    reader = pd.read_csv(
        raw_stream,
        sep=PIPE_DELIMITER,
        chunksize=chunk_rows,
        iterator=True,
        quoting=0,
        keep_default_na=False,
        escapechar=ESCAPE_CHARACTER,
    )

    first_chunk = True
    for chunk_df in reader:
        if first_chunk:
            first_chunk = False
            _LOG.debug(
                "[DBG-8] Parent CSV first chunk — %d rows, %d cols\n"
                "  columns : %s\n"
                "  dtypes  : %s",
                len(chunk_df),
                len(chunk_df.columns),
                list(chunk_df.columns),
                {c: str(t) for c, t in chunk_df.dtypes.items()},
            )
        yield chunk_df
        del chunk_df
        gc.collect()


class _AdlsRawStream(io.RawIOBase):
    """
    A RawIOBase wrapper around an ADLS StorageStreamDownloader.

    Adapts the ADLS download object's read() interface to the RawIOBase
    readinto() contract expected by io.BufferedReader and pandas. Each call
    to readinto() reads exactly len(b) bytes from the downloader and copies
    them into the supplied buffer, returning the number of bytes actually read.
    """

    def __init__(self, downloader):
        """Initialise with an ADLS StorageStreamDownloader instance."""
        super().__init__()
        self._downloader = downloader

    def readable(self) -> bool:
        """Return True to indicate the stream supports read operations."""
        return True

    def readinto(self, b) -> int:
        """
        Read up to len(b) bytes from the ADLS downloader into buffer b.

        Returns the number of bytes written, or 0 at end of stream.
        """
        data = self._downloader.read(len(b))
        if not data:
            return 0
        length = len(data)
        b[:length] = data
        return length


# ============================================================================
# POSTGRESQL HELPERS
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


def get_table_column_types(
    conn, schema: str, table: str
) -> Tuple[Dict[str, str], Dict[str, int]]:
    """
    Return column type and max-length mappings for a table.

    Queries information_schema.columns for the specified schema.table and
    returns two dicts:

    - column_types: maps column_name → PostgreSQL data type string. For ARRAY
      columns a synthetic 'column_name.__elem__' key is also added with the
      element base type (e.g. '_int4' → 'int4') so downstream coercion knows
      the element type when building PostgreSQL array literals.

    - col_max_lengths: maps column_name → character_maximum_length (int) for
      every varchar/char column that has a finite length constraint. Columns
      with no limit (text, unbounded varchar) are omitted. Used by
      _coerce_value_for_pg to silently truncate values that would otherwise
      exceed the column size and cause a COPY error.
    """
    query = """
        SELECT column_name, data_type, udt_name, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name   = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        rows = cur.fetchall()

    column_types: Dict[str, str] = {}
    col_max_lengths: Dict[str, int] = {}
    for col_name, data_type, udt_name, char_max_len in rows:
        dt = data_type.lower()
        column_types[col_name] = dt
        if dt == "array" and udt_name and udt_name.startswith("_"):
            column_types[f"{col_name}.__elem__"] = udt_name[1:].lower()
        if char_max_len is not None:
            col_max_lengths[col_name] = int(char_max_len)
    return column_types, col_max_lengths


def get_server_default_columns(conn, schema: str, table: str) -> Set[str]:
    """
    Return the set of column names that have a server-side DEFAULT expression.

    Queries information_schema.columns for rows where column_default is not
    NULL. Columns with server defaults require special handling during upsert:
    they should only be included in the INSERT column list when the batch
    actually supplies non-null values for them, to avoid overriding the default
    with an explicit NULL.
    """
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema   = %s
          AND table_name     = %s
          AND column_default IS NOT NULL
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        rows = cur.fetchall()
    return {r[0] for r in rows}


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
# DATA RECONSTRUCTION HELPERS
# ============================================================================


def unflatten_dict(flat: Dict[str, Any], sep: str = ".") -> Dict:
    """
    Convert a flat dot-separated key dictionary into a nested dictionary.

    Splits each key on sep and creates intermediate dicts as needed. For
    example, {'a.b.c': 1} becomes {'a': {'b': {'c': 1}}}. If a non-dict value
    already occupies an intermediate path node, it is overwritten with a new
    dict to allow the traversal to continue.
    """
    result: Dict = {}
    for key, value in flat.items():
        parts = key.split(sep)
        cur = result
        for i, part in enumerate(parts):
            if i == len(parts) - 1:
                cur[part] = value
            else:
                if not isinstance(cur.get(part), dict):
                    cur[part] = {}
                cur = cur[part]
    return result


def _python_repr_to_json(s: str) -> str:
    """
    Convert a Python repr-style string to a valid JSON string.

    Rewrites single-quoted strings to double-quoted strings, translates Python
    literal tokens (True → true, False → false, None → null), and leaves
    structural characters and numbers unchanged. This is a best-effort
    character-level transform used as a fallback when json.loads and
    ast.literal_eval both fail to parse a value that originated as a Python
    data-structure repr.
    """
    result = []
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if c == '"':
            result.append('"')
            i += 1
            while i < n:
                ch = s[i]
                if ch == "\\":
                    result.append(ch)
                    i += 1
                    if i < n:
                        result.append(s[i])
                        i += 1
                elif ch == '"':
                    result.append('"')
                    i += 1
                    break
                else:
                    result.append(ch)
                    i += 1
            continue
        if c == "'":
            result.append('"')
            i += 1
            while i < n:
                ch = s[i]
                if ch == "\\":
                    result.append(ch)
                    i += 1
                    if i < n:
                        result.append(s[i])
                        i += 1
                elif ch == "'":
                    result.append('"')
                    i += 1
                    break
                elif ch == '"':
                    result.append('\\"')
                    i += 1
                else:
                    result.append(ch)
                    i += 1
            continue
        if c in "{}[]:,\n\r\t ":
            result.append(c)
            i += 1
            continue
        j = i
        while j < n and s[j] not in "{}[]:,\n\r\t '\"":
            j += 1
        token = s[i:j]
        i = j
        if token == "True":
            result.append("true")
        elif token == "False":
            result.append("false")
        elif token == "None":
            result.append("null")
        else:
            try:
                float(token)
                result.append(token)
            except ValueError:
                result.append('"' + token + '"')
    return "".join(result)


def _try_parse_json_string(v: str) -> Tuple[bool, Any]:
    """
    Attempt to parse a string as JSON using three progressively looser strategies.

    First tries json.loads directly. If that fails, tries ast.literal_eval for
    Python literal syntax. If that also fails, runs _python_repr_to_json to
    convert Python repr tokens to JSON and retries json.loads. Returns a tuple
    of (success: bool, parsed_value). On all failures returns (False, None).
    """
    try:
        return True, json.loads(v)
    except json.JSONDecodeError:
        pass
    try:
        parsed = ast.literal_eval(v)
        if isinstance(parsed, (dict, list, str, int, float, bool)):
            return True, parsed
    except Exception:
        pass
    try:
        return True, json.loads(_python_repr_to_json(v))
    except Exception:
        pass
    return False, None


def _sanitise_for_json(value: Any) -> Any:
    """
    Recursively clean a value so it can be safely serialised to JSON.

    Traverses dicts and lists recursively. For string values that start with
    '{' or '[', attempts to parse them as JSON/Python-repr objects and replaces
    the string with the parsed structure. This handles cases where nested
    objects were double-serialised to strings during CSV export and need to be
    restored to their native structure before re-serialisation.
    """
    if isinstance(value, dict):
        return {k: _sanitise_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitise_for_json(i) for i in value]
    if isinstance(value, str):
        stripped = value.strip()
        if stripped and stripped[0] in ("{", "["):
            ok, parsed = _try_parse_json_string(stripped)
            if ok and isinstance(parsed, (dict, list)):
                return _sanitise_for_json(parsed)
    return value


def dict_to_json_string(d: Any) -> str:
    """
    Serialise a value to a JSON string after sanitising nested structures.

    Runs _sanitise_for_json on the input to resolve any embedded string-encoded
    JSON, then calls json.dumps with default=str (to handle non-serialisable
    types such as datetime) and ensure_ascii=False to preserve Unicode. Returns
    '{}' if serialisation fails for any reason.
    """
    try:
        return json.dumps(_sanitise_for_json(d), default=str, ensure_ascii=False)
    except Exception:
        return "{}"


def dict_to_xml(tag: str, d: Any) -> str:
    """
    Serialise a dict (or other value) to an XML string under the given root tag.

    If d is a dict and contains a '_xml_root_tag_' key, that value overrides
    the supplied tag as the root element name. Delegates element construction
    to _build_xml_element and serialises the result to a Unicode string using
    ElementTree with short_empty_elements=False so all tags have explicit
    closing tags.
    """
    if isinstance(d, dict):
        tag = d.pop("_xml_root_tag_", tag)
    root = _build_xml_element(tag, d)
    return ET.tostring(root, encoding="unicode", short_empty_elements=False)


def _sanitise_xml_tag(tag: str) -> str:
    """
    Convert an arbitrary string to a valid XML element name.

    Replaces any character that is not alphanumeric, a dot, hyphen, or
    underscore with an underscore. Prepends an underscore if the result starts
    with a digit or hyphen. Falls back to '_' if the result is empty.
    """
    safe = re.sub(r"[^\w.\-]", "_", str(tag)) if tag else "_"
    if safe and (safe[0].isdigit() or safe[0] == "-"):
        safe = "_" + safe
    return safe or "_"


def _build_xml_element(tag: str, value: Any) -> Element:
    """
    Recursively build an xml.etree.ElementTree.Element from a Python value.

    Sanitises the tag name, then populates the element based on the type of
    value: dicts are expanded via _populate_element_from_dict, lists produce
    sibling child elements each sharing the parent tag, and scalars become text
    content. String values that are valid JSON arrays of primitives are expanded
    into sibling child elements rather than stored as a raw string.
    """
    safe_tag = _sanitise_xml_tag(tag)
    elem = Element(safe_tag)
    if value is None:
        return elem
    if isinstance(value, dict):
        _populate_element_from_dict(elem, value)
    elif isinstance(value, list):
        for item in value:
            elem.append(_build_xml_element(safe_tag, item))
    else:
        s = str(value)
        stripped = s.strip()
        if stripped.startswith("["):
            try:
                items = json.loads(stripped)
                if isinstance(items, list) and all(
                    not isinstance(i, (dict, list)) for i in items
                ):
                    for item in items:
                        child = Element(safe_tag)
                        child.text = "" if item is None else str(item)
                        elem.append(child)
                    return elem
            except (json.JSONDecodeError, ValueError):
                pass
        elem.text = s
    return elem


def _populate_element_from_dict(elem: Element, d: Dict) -> None:
    """
    Populate an existing Element with children derived from a dict.

    Processes each key-value pair: keys beginning with '@' become XML
    attributes; '_text' sets the element's text content; list values produce
    repeated child elements; dict values produce a single nested child element;
    and scalar values produce a single child text element. String scalars that
    begin with '[' and parse as JSON arrays of primitives are expanded into
    repeated sibling child elements. The reserved key '_xml_root_tag_' is
    skipped entirely.
    """
    for key, val in d.items():
        if key == "_xml_root_tag_":
            continue
        if key.startswith("@"):
            elem.set(_sanitise_xml_tag(key[1:]), str(val) if val is not None else "")
        elif key == "_text":
            elem.text = str(val) if val is not None else ""
        elif isinstance(val, list):
            for item in val:
                child = SubElement(elem, _sanitise_xml_tag(key))
                if isinstance(item, dict):
                    _populate_element_from_dict(child, item)
                else:
                    child.text = str(item) if item is not None else ""
        elif isinstance(val, dict):
            child = SubElement(elem, _sanitise_xml_tag(key))
            _populate_element_from_dict(child, val)
        else:
            s = str(val) if val is not None else ""
            stripped = s.strip()
            if stripped.startswith("["):
                try:
                    items = json.loads(stripped)
                    if isinstance(items, list) and all(
                        not isinstance(i, (dict, list)) for i in items
                    ):
                        for item in items:
                            child = SubElement(elem, _sanitise_xml_tag(key))
                            child.text = "" if item is None else str(item)
                        continue
                except (json.JSONDecodeError, ValueError):
                    pass
            child = SubElement(elem, _sanitise_xml_tag(key))
            child.text = s


# ============================================================================
# CHILD CSV STREAMING
# ============================================================================


def _build_rid_to_parent_mapping(batch_parents: List[Dict]) -> Dict[str, Dict]:
    """
    Build a lookup dict mapping each parent's string _rid to its full object.

    Filters out any parent dict that lacks a non-empty '_rid' value. The
    resulting mapping is used to quickly look up parent rows when assigning
    child objects and to supply the set of parent RIDs for child CSV filtering.
    """
    return {str(p["_rid"]): p for p in batch_parents if p.get("_rid")}


def process_child_csv_streaming(
    service_client,
    file_system: str,
    full_path: str,
    parent_rids: Set[str],
    col_name: str,
    all_objects_by_rid: Dict,
    child_objects_by_table: Dict,
    chunk_size: int = CHILD_CHUNK_SIZE,
) -> Tuple[int, Set[str]]:
    """
    Stream a child CSV file from ADLS and collect rows matching the given parent RIDs.

    Downloads the file identified by full_path, reads it in chunk_size-row
    chunks (driven by the caller-supplied batch_size so all pipeline stages use
    the same window), and filters each chunk to rows whose '_parent_rid' is in
    parent_rids. Each matching row is converted to a nested dict via
    _build_child_object_from_row, appended to child_objects_by_table[col_name],
    and registered in all_objects_by_rid by its own '_rid'. Returns a tuple of
    (total matched row count, set of child RIDs found). Raises on any download
    or parsing error.
    """
    try:
        fs_client = service_client.get_file_system_client(file_system)
        file_client = fs_client.get_file_client(full_path)
        download = file_client.download_file()

        child_rids: Set[str] = set()
        row_count = 0
        child_objects_by_table.setdefault(col_name, [])

        for chunk in pd.read_csv(
            download,
            sep=PIPE_DELIMITER,
            chunksize=chunk_size,
            iterator=True,
            quoting=0,
            keep_default_na=False,
            escapechar=ESCAPE_CHARACTER,
        ):
            if "_parent_rid" not in chunk.columns:
                del chunk
                continue

            chunk["_parent_rid"] = chunk["_parent_rid"].astype(str)
            filtered = chunk[chunk["_parent_rid"].isin(parent_rids)]
            _LOG.debug(
                "[DBG-7] child CSV '%s' col='%s'  chunk=%d rows  matched=%d",
                full_path,
                col_name,
                len(chunk),
                len(filtered),
            )
            del chunk

            if filtered.empty:
                del filtered
                continue

            if "_rid" in filtered.columns:
                filtered["_rid"] = filtered["_rid"].astype(str)

            for _, row in filtered.iterrows():
                obj = _build_child_object_from_row(row)
                child_objects_by_table[col_name].append(obj)
                if obj.get("_rid"):
                    child_rids.add(str(obj["_rid"]))
                    all_objects_by_rid[str(obj["_rid"])] = obj
                row_count += 1

            del filtered

        return row_count, child_rids

    except Exception as e:
        _LOG.error(f"Error streaming child CSV '{full_path}': {str(e)}")
        raise


def _build_child_object_from_row(row: pd.Series) -> Dict:
    """
    Convert a pandas Series representing a child CSV row into a nested dict.

    Extracts '_rid' and '_parent_rid' as string fields (using None for missing
    or NaN values). Separates remaining non-null fields into '_has_array_*'
    marker columns and regular data columns. Unflattens the regular columns
    into a nested dict via unflatten_dict, merges the array markers back in,
    and restores '_rid' and '_parent_rid' at the top level.
    """
    obj: Dict = {
        "_rid": (
            str(row["_rid"])
            if "_rid" in row.index and pd.notna(row.get("_rid"))
            else None
        ),
        "_parent_rid": (
            str(row["_parent_rid"])
            if "_parent_rid" in row.index and pd.notna(row.get("_parent_rid"))
            else None
        ),
    }
    has_array_fields = {}
    regular_fields = {}
    for k, v in row.items():
        if k in ("_rid", "_parent_rid") or not pd.notna(v):
            continue
        if str(k).startswith("_has_array_"):
            has_array_fields[k] = v
        else:
            regular_fields[k] = v

    nested = unflatten_dict(regular_fields)
    nested.update(has_array_fields)
    nested["_rid"] = obj["_rid"]
    nested["_parent_rid"] = obj["_parent_rid"]
    return nested


# ============================================================================
# CSV PATH ORGANISATION
# ============================================================================


def _nearest_ancestor_csv(col_name: str, all_col_names: Set[str]) -> Optional[str]:
    """
    Return the col_name of the nearest ancestor that has its own CSV file.

    Walks up the dot-separated path segments from the immediate parent toward
    the root, returning the first ancestor whose full dot-path exists in
    all_col_names. Returns None when no ancestor has its own CSV, meaning
    the direct parent is the main table rather than another child CSV.

    Example with all_col_names = {"preferences", "preferences.shopping_behavior.last_5_orders"}:
      "preferences"                                   → None
      "preferences.profile.loyalty.history"           → "preferences"
      "preferences.shopping_behavior.last_5_orders"   → "preferences"
      "preferences.shopping_behavior.last_5_orders.items"
                                                      → "preferences.shopping_behavior.last_5_orders"
    """
    parts = col_name.split(".")
    for i in range(len(parts) - 1, 0, -1):
        ancestor = ".".join(parts[:i])
        if ancestor in all_col_names:
            return ancestor
    return None


def _csv_depth(col_name: str, all_col_names: Set[str]) -> int:
    """
    Compute the filter depth of a child CSV using the nearest-ancestor-CSV algorithm.

    Depth 0 means the CSV is a direct child of the parent table (filtered by
    entity _rids). Depth N means it is a child of a depth-(N-1) CSV row.
    Depth is determined recursively: if no ancestor CSV exists, depth is 0;
    otherwise depth is the ancestor's depth plus one.

    This replaces a previous approach that used col_name.count("."), which
    counted dots in the JSON path and was unrelated to actual CSV nesting depth.

    Example depths:
      "preferences"                                       → 0
      "preferences.profile.loyalty.history"               → 1
      "preferences.shopping_behavior.devices"             → 1
      "preferences.shopping_behavior.last_5_orders"       → 1
      "preferences.shopping_behavior.last_5_orders.items" → 2
    """
    ancestor = _nearest_ancestor_csv(col_name, all_col_names)
    if ancestor is None:
        return 0
    return _csv_depth(ancestor, all_col_names) + 1


def _organize_csv_paths_by_depth(
    csv_paths: List[str],
    table_dir: str,
    table_name: str,
) -> Dict[int, List[Dict]]:
    """
    Organise a list of child CSV paths into a dict keyed by their filter depth.

    Skips the parent table CSV itself. For each remaining path, derives the
    col_name from the relative path segments (everything between the table
    directory prefix and the final filename). Collects all col_names first so
    that _csv_depth can correctly resolve ancestor relationships, then assigns
    each path its depth. Returns a dict mapping depth integer to a list of info
    dicts, each containing 'full_path', 'col_name', and 'depth'.
    """
    parent_csv = f"{table_dir}/{table_name}.csv"

    col_name_set: Set[str] = set()
    path_to_col: Dict[str, str] = {}
    for path in csv_paths:
        if path == parent_csv or not path.startswith(table_dir + "/"):
            continue
        rel = path[len(table_dir) + 1 :]
        parts = [p for p in rel.split("/") if p]
        if not parts:
            continue
        col_name = (
            ".".join(parts[:-1]) if len(parts) > 1 else parts[0].rsplit(".", 1)[0]
        )
        col_name_set.add(col_name)
        path_to_col[path] = col_name

    result: Dict[int, List[Dict]] = {}
    for path in sorted(csv_paths):
        col_name = path_to_col.get(path)
        if col_name is None:
            continue
        depth = _csv_depth(col_name, col_name_set)
        result.setdefault(depth, []).append(
            {"full_path": path, "col_name": col_name, "depth": depth}
        )

    return result


# ============================================================================
# ARRAY / CHILD RECONSTRUCTION
# ============================================================================


def _initialize_arrays_from_markers(all_objects: Dict) -> None:
    """
    Pre-initialise nested array slots based on '_has_array_*' marker keys.

    For each object in all_objects, finds any key that begins with
    '_has_array_'. Interprets the remainder of the key as a dot-separated path
    to a nested array slot and ensures that path exists as an empty list ([]
    if the slot is absent). The marker key is then removed from the object.
    This guarantees that array fields appear as [] rather than being absent
    when no child rows exist for a given parent.
    """
    for obj in all_objects.values():
        for marker in [k for k in list(obj.keys()) if k.startswith("_has_array_")]:
            parts = marker[len("_has_array_") :].split(".")
            target = obj
            ok = True
            for part in parts[:-1]:
                if part not in target:
                    target[part] = {}
                elif not isinstance(target[part], dict):
                    ok = False
                    break
                target = target[part]
            if ok and parts[-1] not in target:
                target[parts[-1]] = []
            obj.pop(marker, None)


def _assign_children_to_parents(
    child_objects_by_table: Dict[str, List[Dict]],
    all_objects: Dict,
) -> None:
    """
    Place each child object into the correct nested array field of its parent row.

    Processes child tables in ascending depth order (shallowest first) to ensure
    parent rows are fully populated before their children are attached.

    For each col_name, computes the LOCAL navigation path within the immediate
    parent CSV row by stripping the nearest-ancestor-CSV prefix. For example,
    if col_name is "preferences.profile.loyalty.history" and the ancestor CSV
    is "preferences", the local path is ["profile", "loyalty", "history"] and
    the array key is "history". Navigation proceeds only through the local path
    (not the full dot-path from the table root), avoiding phantom sub-dict
    creation that occurred when the full path was used on a parent row that
    already had the ancestor prefix stripped.

    Internal fields (_rid, _parent_rid, _has_array_*) are removed from each
    child before it is appended to the parent array.
    """
    all_col_names: Set[str] = set(child_objects_by_table.keys())

    def _sort_key(item: Tuple) -> int:
        return _csv_depth(item[0], all_col_names)

    for col_name, objects in sorted(child_objects_by_table.items(), key=_sort_key):
        if not objects:
            continue

        ancestor = _nearest_ancestor_csv(col_name, all_col_names)
        if ancestor is None:
            local_parts = [col_name.split(".")[-1]]
        else:
            local_suffix = col_name[len(ancestor) + 1 :]
            local_parts = local_suffix.split(".")

        arr_key = local_parts[-1]
        nav_path = local_parts[:-1]

        grouped: Dict[str, List[Dict]] = {}
        for obj in objects:
            prid = str(obj.get("_parent_rid", ""))
            if prid and prid != "None":
                grouped.setdefault(prid, []).append(obj)

        for prid, children in grouped.items():
            parent = all_objects.get(prid)
            if parent is None:
                continue

            clean_children = []
            for child in children:
                crid = str(child.get("_rid", ""))
                clean = {
                    k: v
                    for k, v in child.items()
                    if k not in ("_rid", "_parent_rid")
                    and not k.startswith("_has_array_")
                }
                clean_children.append(clean)
                if crid and crid in all_objects:
                    all_objects[crid] = clean

            target = parent
            for part in nav_path:
                if not isinstance(target, dict):
                    break
                target = target.setdefault(part, {})

            if isinstance(target, dict):
                existing = target.get(arr_key)
                if isinstance(existing, list):
                    existing.extend(clean_children)
                else:
                    target[arr_key] = clean_children


def _strip_internal_fields(obj: Any) -> Any:
    """
    Recursively remove internal tracking fields from a nested object.

    Removes '_rid', '_parent_rid', and any key beginning with '_has_array_'
    from every dict encountered during traversal. Lists are traversed
    element-wise. Non-dict, non-list values are returned unchanged. Used to
    clean child objects before serialising them into JSON or XML column values.
    """
    if isinstance(obj, dict):
        return {
            k: _strip_internal_fields(v)
            for k, v in obj.items()
            if k not in ("_rid", "_parent_rid") and not k.startswith("_has_array_")
        }
    if isinstance(obj, list):
        return [_strip_internal_fields(i) for i in obj]
    return obj


# ============================================================================
# STRUCTURED COLUMN RECONSTRUCTION
# ============================================================================


def _find_child_for_parent(
    child_table: str,
    parent_rid: str,
    child_objects_by_table: Dict,
) -> Optional[Dict]:
    """
    Return the first child object in child_table whose '_parent_rid' matches parent_rid.

    Iterates through child_objects_by_table[child_table] and returns the first
    object where str(_parent_rid) == parent_rid. Returns None if the table has
    no entries or no matching object is found.
    """
    for obj in child_objects_by_table.get(child_table, []):
        if str(obj.get("_parent_rid", "")) == parent_rid:
            return obj
    return None


def reconstruct_structured_columns(
    parent_row: Dict,
    child_objects_by_table: Dict,
    column_types: Dict,
    all_objects: Dict,
    rid_to_parent: Dict,
) -> Dict:
    """
    Reconstruct the final column values for a parent row, resolving JSON and XML fields.

    Iterates the parent row's key-value pairs and handles three cases:

    1. Keys prefixed with 'has_json_': the suffix is a JSON column name. When the
       flag is true, locates the matching child object by _parent_rid and serialises
       it to a JSON string via dict_to_json_string. When false, uses the raw value
       already present on the parent row. Emits '{}' when the flag is true but no
       child object is found.

    2. Keys prefixed with 'has_xml_': same logic but serialises via dict_to_xml.
       Emits '<col_name/>' when the flag is true but no child object is found.

    3. All other keys that are not themselves JSON/XML column types (those are
       handled through their has_json_/has_xml_ flags) are passed through unchanged.

    Internal '_rid' and '_parent_rid' keys are omitted from the output.
    """
    row_rid = str(parent_row.get("_rid", ""))
    result: Dict = {}

    for key, value in parent_row.items():
        if key in ("_rid", "_parent_rid"):
            continue

        if key.startswith("has_json_"):
            col = key[len("has_json_") :]
            is_on = str(value).lower() in ("true", "1")
            child = (
                _find_child_for_parent(col, row_rid, child_objects_by_table)
                if is_on
                else None
            )
            _LOG.debug(
                "[DBG-5] has_json_ col='%s' flag=%s child_found=%s",
                col,
                value,
                child is not None,
            )
            if is_on:
                clean = _strip_internal_fields(child) if child else None
                result[col] = dict_to_json_string(clean) if clean else "{}"
            else:
                result[col] = parent_row.get(col)

        elif key.startswith("has_xml_"):
            col = key[len("has_xml_") :]
            is_on = str(value).lower() in ("true", "1")
            child = (
                _find_child_for_parent(col, row_rid, child_objects_by_table)
                if is_on
                else None
            )
            _LOG.debug(
                "[DBG-5] has_xml_ col='%s' flag=%s child_found=%s",
                col,
                value,
                child is not None,
            )
            if is_on:
                result[col] = (
                    dict_to_xml(col, _strip_internal_fields(child))
                    if child
                    else f"<{col}/>"
                )
            else:
                result[col] = parent_row.get(col)

        elif key in column_types and column_types[key] in JSON_TYPES | XML_TYPES:
            pass

        else:
            result[key] = value

    return result


# ============================================================================
# UPSERT
# ============================================================================


def _coerce_value_for_pg(value: Any, col_type: str, max_length: Optional[int] = None) -> Any:
    """
    Coerce a Python value to the appropriate string representation for a PostgreSQL column.

    Handles universal NULL detection first (None, pandas NA/NaN, empty/null strings).
    Then dispatches to type-specific coercion based on col_type membership in the
    defined type sets. Returns None for NULL values (which COPY maps to the empty
    string with NULL ''). For unrecognised types, returns the value as-is.

    When max_length is provided (sourced from information_schema
    character_maximum_length), string results for TEXT_TYPES are silently
    truncated to that length before being written to the COPY buffer. This
    prevents "value too long for type character varying(N)" errors when the
    source data (e.g. encrypted or encoded values) exceeds the column
    constraint.

    Type-specific behaviours:
    - JSON/JSONB: parses strings, sanitises nested values, dumps to JSON string.
    - XML: strips and returns the string, or None if empty.
    - BOOLEAN: maps common truthy/falsy strings and numeric values to 'true'/'false'.
    - INT: converts via float to handle '1.0'-style representations.
    - FLOAT: uses repr() to preserve precision.
    - NUMERIC/DECIMAL: uses Python Decimal for exact formatting.
    - UUID: validates and normalises UUID format.
    - DATE: formats as YYYY-MM-DD using dateutil if needed.
    - TIMESTAMP: formats as ISO 8601 with space separator.
    - TIME: returns HH:MM:SS.ffffff or the raw string.
    - INTERVAL: returns the string as-is.
    - BYTEA: converts bytes to \\x hex notation.
    - ARRAY: delegates to _coerce_pg_array.
    - TEXT types: converts to str, then truncates to max_length characters if set.
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, str) and value.strip().lower() in PD_NULL_STRINGS:
        return None

    if col_type in JSON_TYPES:
        if isinstance(value, (dict, list)):
            return _sanitise_copy_cell(
                json.dumps(_sanitise_for_json(value), default=str, ensure_ascii=False)
            )
        if isinstance(value, str):
            v = value.strip()
            if not v:
                return None
            ok, parsed = _try_parse_json_string(v)
            if ok:
                return _sanitise_copy_cell(
                    json.dumps(
                        _sanitise_for_json(parsed), default=str, ensure_ascii=False
                    )
                )
            return None
        return _sanitise_copy_cell(json.dumps(value, default=str, ensure_ascii=False))

    if col_type in XML_TYPES:
        s = str(value).strip()
        return _sanitise_copy_cell(s) if s else None

    if col_type in BOOL_TYPES:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return "true" if value else "false"
        if isinstance(value, str):
            v = value.strip().lower()
            if v in ("true", "t", "yes", "y", "1", "on"):
                return "true"
            if v in ("false", "f", "no", "n", "0", "off"):
                return "false"
        return None

    if col_type in INT_TYPES:
        try:
            f = float(value)
            if f != f or f in (float("inf"), float("-inf")):
                return None
            return str(int(f))
        except (ValueError, TypeError):
            return None

    if col_type in FLOAT_TYPES:
        try:
            f = float(value)
            if f != f or f in (float("inf"), float("-inf")):
                return None
            return repr(f)
        except (ValueError, TypeError):
            return None

    if col_type in NUMERIC_TYPES:
        try:
            from decimal import Decimal

            return format(Decimal(str(value).strip()), "f")
        except Exception:
            return None

    if col_type in UUID_TYPES:
        import uuid as _uuid_mod

        s = str(value).strip()
        try:
            return str(_uuid_mod.UUID(s))
        except ValueError:
            return None

    if col_type in DATE_TYPES:
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d")
        s = str(value).strip()
        if s.lower() in ("nat", "nan", "none", "null", ""):
            return None
        try:
            from dateutil import parser as _du

            return _du.parse(s).strftime("%Y-%m-%d")
        except Exception:
            return None

    if col_type in TIMESTAMP_TYPES:
        if hasattr(value, "isoformat"):
            return value.isoformat(sep=" ")
        s = str(value).strip()
        if s.lower() in ("nat", "nan", "none", "null", ""):
            return None
        try:
            from dateutil import parser as _du

            return _du.parse(s).isoformat(sep=" ")
        except Exception:
            return None

    if col_type in TIME_TYPES:
        if hasattr(value, "strftime"):
            return (
                value.strftime("%H:%M:%S.%f")
                if hasattr(value, "microsecond")
                else str(value)
            )
        s = str(value).strip()
        return None if s.lower() in ("nat", "nan", "none", "null", "") else s

    if col_type in INTERVAL_TYPES:
        return str(value).strip() or None

    if col_type in BYTES_TYPES:
        if isinstance(value, (bytes, bytearray)):
            return "\\x" + value.hex()
        return str(value).strip() or None

    if col_type in ARRAY_TYPES:
        return _coerce_pg_array(value)

    if col_type in TEXT_TYPES:
        s = str(value)
        if max_length is not None and len(s) > max_length:
            _LOG.debug(
                "Truncating value for text column: original length=%d, max_length=%d",
                len(s),
                max_length,
            )
            return s[:max_length]
        return s

    return value


def _coerce_pg_array(value: Any) -> Optional[str]:
    """
    Convert a Python value to a PostgreSQL array literal string.

    Handles None and pandas NA as NULL. Python lists are serialised via
    _list_to_pg_array. Strings that already begin with '{' are returned as-is
    (assumed to be pre-formatted PG array literals). Strings beginning with '['
    are treated as JSON arrays and parsed before serialisation. Any other scalar
    is wrapped in a single-element list.
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, list):
        return _list_to_pg_array(value)
    if isinstance(value, str):
        s = value.strip()
        if not s or s.lower() in PD_NULL_STRINGS:
            return None
        if s.startswith("{"):
            return s
        if s.startswith("["):
            ok, parsed = _try_parse_json_string(s)
            if ok and isinstance(parsed, list):
                return _list_to_pg_array(parsed)
    return _list_to_pg_array([value])


def _list_to_pg_array(lst: list) -> str:
    """
    Serialise a Python list to a PostgreSQL array literal string.

    Produces a string of the form {elem1,elem2,...}. None and pandas NA become
    NULL. Booleans become TRUE/FALSE. Numbers are converted with str(). Nested
    lists are recursively serialised. All other values are double-quoted with
    internal backslashes and double-quotes escaped.
    """
    parts = []
    for item in lst:
        if item is None:
            parts.append("NULL")
            continue
        try:
            if pd.isna(item):
                parts.append("NULL")
                continue
        except (TypeError, ValueError):
            pass
        if isinstance(item, bool):
            parts.append("TRUE" if item else "FALSE")
        elif isinstance(item, (int, float)):
            parts.append(str(item))
        elif isinstance(item, list):
            parts.append(_list_to_pg_array(item))
        else:
            escaped = str(item).replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'"{escaped}"')
    return "{" + ",".join(parts) + "}"


def _build_upsert_sql(
    schema: str, table: str, columns: List[str], pk_columns: List[str]
) -> str:
    """
    Build an INSERT … ON CONFLICT … DO UPDATE (upsert) SQL statement.

    Uses a temporary staging table named _stage (created by the caller) as the
    data source. Three variants are produced depending on the inputs:

    1. No primary key: plain INSERT without conflict handling.
    2. Primary key but no non-PK columns: INSERT … ON CONFLICT DO NOTHING.
    3. Primary key with non-PK columns: INSERT … ON CONFLICT DO UPDATE SET
       for every non-PK column (full upsert semantics).

    All identifiers are double-quoted for safety.
    """
    quoted_schema = f'"{schema}"'
    quoted_table = f'"{table}"'
    quoted_columns = ", ".join(f'"{c}"' for c in columns)
    quoted_pk = ", ".join(f'"{c}"' for c in pk_columns)
    non_pk_columns = [c for c in columns if c not in pk_columns]

    if not pk_columns:
        return f"INSERT INTO {quoted_schema}.{quoted_table} ({quoted_columns}) SELECT {quoted_columns} FROM _stage"
    if not non_pk_columns:
        return (
            f"INSERT INTO {quoted_schema}.{quoted_table} ({quoted_columns}) "
            f"SELECT {quoted_columns} FROM _stage ON CONFLICT ({quoted_pk}) DO NOTHING"
        )
    update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_pk_columns)
    return (
        f"INSERT INTO {quoted_schema}.{quoted_table} ({quoted_columns}) "
        f"SELECT {quoted_columns} FROM _stage "
        f"ON CONFLICT ({quoted_pk}) DO UPDATE SET {update_set}"
    )


def upsert_batch(
    conn,
    schema: str,
    table: str,
    rows: List[Dict],
    column_types: Dict[str, str],
    pk_columns: List[str],
    table_columns: List[str],
    server_default_cols: Optional[Set[str]] = None,
    col_max_lengths: Optional[Dict[str, int]] = None,
) -> Tuple[int, int, List[str]]:
    """
    Write a batch of rows into a PostgreSQL table using COPY + upsert.

    Determines the set of columns to insert: starts from the intersection of
    table_columns and column_types (excluding synthetic '.__elem__' keys), then
    drops server-default columns unless the batch contains at least one non-null
    value for them.

    For each row, _coerce_value_for_pg is called per column to produce the
    correct string representation. For text/varchar columns, col_max_lengths
    supplies the character_maximum_length from information_schema so that values
    exceeding the column constraint are silently truncated before being written
    to the COPY buffer, preventing "value too long for type character varying(N)"
    errors. The results are written to an in-memory CSV buffer using csv.QUOTE_ALL
    (eliminating field-boundary ambiguity), which is fed to PostgreSQL via COPY
    INTO a temporary _stage table. The upsert SQL built by _build_upsert_sql is
    then executed to move rows from _stage into the target table.

    Retries up to PG_RETRY_MAX times on psycopg2.OperationalError with
    exponential back-off starting at PG_RETRY_BASE_SEC seconds. Any other
    exception causes an immediate rollback and returns (0, len(rows), [error]).

    Returns (successful_count, failed_count, error_messages).
    """
    if not rows:
        return 0, 0, []

    if server_default_cols is None:
        server_default_cols = set()
    if col_max_lengths is None:
        col_max_lengths = {}

    candidate_cols = [
        c for c in table_columns if c in column_types and ".__elem__" not in c
    ]

    supplied_cols: Set[str] = set()
    for row in rows:
        for col in candidate_cols:
            if col in server_default_cols and col not in supplied_cols:
                raw = row.get(col)
                is_null = raw is None or (
                    isinstance(raw, str) and raw.strip().lower() in PD_NULL_STRINGS
                )
                try:
                    is_null = is_null or bool(pd.isna(raw))
                except (TypeError, ValueError):
                    pass
                if not is_null:
                    supplied_cols.add(col)

    insert_cols = [
        c for c in candidate_cols if c not in server_default_cols or c in supplied_cols
    ]

    if not insert_cols:
        return 0, len(rows), ["No overlapping columns between CSV and target table"]

    _LOG.debug(
        "[DBG-4] upsert_batch table=%s.%s rows=%d insert_cols=%s pk=%s",
        schema,
        table,
        len(rows),
        insert_cols,
        pk_columns,
    )

    quoted_schema = f'"{schema}"'
    quoted_table = f'"{table}"'

    for attempt in range(PG_RETRY_MAX):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TEMP TABLE _stage (LIKE {quoted_schema}.{quoted_table} INCLUDING DEFAULTS) ON COMMIT DROP"
                )

                buf = io.StringIO()
                writer = csv.writer(
                    buf,
                    delimiter=PIPE_DELIMITER,
                    quoting=csv.QUOTE_ALL,
                    quotechar='"',
                    lineterminator="\n",
                )

                suspect_rows: List[Tuple[int, str, str]] = []
                for row_idx, row in enumerate(rows):
                    cells = []
                    for col in insert_cols:
                        val = _coerce_value_for_pg(
                            row.get(col),
                            column_types.get(col, ""),
                            max_length=col_max_lengths.get(col),
                        )
                        cells.append("" if val is None else str(val))

                    if row_idx < COPY_BUFFER_DEBUG_ROWS:
                        row_buf = io.StringIO()
                        csv.writer(
                            row_buf,
                            delimiter=PIPE_DELIMITER,
                            quoting=csv.QUOTE_ALL,
                            quotechar='"',
                            lineterminator="\n",
                        ).writerow(cells)
                        _LOG.debug(
                            "[DBG-4] COPY buffer row %d: %s",
                            row_idx,
                            _preview(
                                row_buf.getvalue().rstrip("\n"), COPY_ROW_PREVIEW_LENGTH
                            ),
                        )

                    writer.writerow(cells)
                    for col_idx, col in enumerate(insert_cols):
                        if _is_suspect_json(cells[col_idx]):
                            suspect_rows.append(
                                (
                                    row_idx,
                                    col,
                                    _preview(
                                        cells[col_idx], SUSPECT_JSON_PREVIEW_LENGTH
                                    ),
                                )
                            )

                if suspect_rows:
                    _LOG.error("[DBG-4] %d SUSPECT JSON CELLS:", len(suspect_rows))
                    for row_idx, col, prev in suspect_rows:
                        _LOG.error(
                            "[DBG-4]   row=%d col='%s' value=%s", row_idx, col, prev
                        )

                buf.seek(0)
                col_list = ", ".join(f'"{c}"' for c in insert_cols)
                cur.copy_expert(
                    f"COPY _stage ({col_list}) FROM STDIN WITH (FORMAT CSV, DELIMITER '{PIPE_DELIMITER}', NULL '')",
                    buf,
                )
                cur.execute(_build_upsert_sql(schema, table, insert_cols, pk_columns))

            conn.commit()
            return len(rows), 0, []

        except psycopg2.OperationalError as e:
            conn.rollback()
            if attempt < PG_RETRY_MAX - 1:
                wait = PG_RETRY_BASE_SEC * (2**attempt)
                _LOG.warning(
                    "Transient PG error (attempt %d/%d): %s. Retrying in %.1fs",
                    attempt + 1,
                    PG_RETRY_MAX,
                    str(e)[:120],
                    wait,
                )
                time.sleep(wait)
            else:
                return 0, len(rows), [str(e)[:200]]
        except Exception as e:
            conn.rollback()
            _LOG.error("Upsert error: %s", str(e))
            return 0, len(rows), [str(e)[:200]]

    return 0, len(rows), ["Max retries exceeded"]


# ============================================================================
# BATCH PROCESSING
# ============================================================================


def process_batch(
    batch_parents_df,
    service_client,
    adls_file_system: str,
    csv_paths: List[str],
    table_dir: str,
    table_name: str,
    conn,
    schema: str,
    column_types: Dict[str, str],
    pk_columns: List[str],
    table_columns: List[str],
    batch_size: int = DEFAULT_BATCH_SIZE,
    server_default_cols: Optional[Set[str]] = None,
    col_max_lengths: Optional[Dict[str, int]] = None,
) -> Tuple[int, int, List[str], int]:
    """
    Process one chunk of parent rows end-to-end: load children, reconstruct, upsert.

    1. Converts each row of batch_parents_df from a flat pandas Series to a
       nested dict (unflattening dot-separated column names and preserving
       _has_array_* markers).
    2. Builds a rid → parent dict and collects the set of parent RIDs.
    3. Organises child CSV paths by depth and streams each child CSV in
       chunk_size=batch_size rows, so the child read window matches the
       parent batch window exactly.
    4. Initialises empty arrays for _has_array_ markers and assigns all child
       objects to their parents.
    5. Calls reconstruct_structured_columns on each parent to resolve has_json_
       and has_xml_ flags into serialised column values.
    6. Calls upsert_batch to write the reconstructed rows to PostgreSQL.
       col_max_lengths is forwarded so that varchar/char columns are truncated
       to their declared size before the COPY write.
    7. Frees memory and calls gc.collect().

    Returns (successful_count, failed_count, error_messages, total_child_rows).
    """
    if server_default_cols is None:
        server_default_cols = set()
    if col_max_lengths is None:
        col_max_lengths = {}

    batch_parents: List[Dict] = []
    for _, row in batch_parents_df.iterrows():
        flat = {k: v for k, v in row.dropna().items()}
        has_array_fields = {
            k: v for k, v in flat.items() if k.startswith("_has_array_")
        }
        regular_fields = {
            k: v for k, v in flat.items() if not k.startswith("_has_array_")
        }
        nested = unflatten_dict(regular_fields)
        nested.update(has_array_fields)
        batch_parents.append(nested)

    rid_to_parent = _build_rid_to_parent_mapping(batch_parents)
    all_objects = dict(rid_to_parent)
    parent_rids = set(rid_to_parent.keys())

    csv_info_by_depth = _organize_csv_paths_by_depth(csv_paths, table_dir, table_name)
    max_depth = max(csv_info_by_depth.keys()) if csv_info_by_depth else -1

    child_objects_by_table: Dict[str, List[Dict]] = {}
    rids_by_depth: Dict[int, Set[str]] = {0: parent_rids}
    total_child_rows = 0

    for depth in range(max_depth + 1):
        if depth not in csv_info_by_depth:
            continue
        rids_by_depth.setdefault(depth + 1, set())
        for info in csv_info_by_depth[depth]:
            filter_rids = parent_rids if depth == 0 else rids_by_depth.get(depth, set())
            if not filter_rids:
                continue
            try:
                count, child_rids = process_child_csv_streaming(
                    service_client,
                    adls_file_system,
                    info["full_path"],
                    filter_rids,
                    info["col_name"],
                    all_objects,
                    child_objects_by_table,
                    chunk_size=batch_size,  # ← propagate batch_size to child reads
                )
                total_child_rows += count
                rids_by_depth[depth + 1].update(child_rids)
            except Exception as e:
                _LOG.error(f"Skipping child CSV '{info['full_path']}': {str(e)}")

    _initialize_arrays_from_markers(all_objects)
    _assign_children_to_parents(child_objects_by_table, all_objects)

    reconstructed: List[Dict] = []
    for parent in batch_parents:
        try:
            rec = reconstruct_structured_columns(
                parent, child_objects_by_table, column_types, all_objects, rid_to_parent
            )
            reconstructed.append(rec)
        except Exception as e:
            _LOG.error(f"Reconstruction error for _rid={parent.get('_rid')}: {str(e)}")

    successful, failed, errors = upsert_batch(
        conn,
        schema,
        table_name,
        reconstructed,
        column_types,
        pk_columns,
        table_columns,
        server_default_cols=server_default_cols,
        col_max_lengths=col_max_lengths,
    )

    del batch_parents, all_objects, child_objects_by_table, reconstructed
    gc.collect()

    return successful, failed, errors, total_child_rows


# ============================================================================
# PARAMETER VALIDATION
# ============================================================================


def validate_and_extract_params(params: dict) -> dict:
    """
    Validate the input parameter dict and return a normalised copy.

    Checks that all required keys are present and non-empty. Coerces pg_port
    and batch_size to integers, falling back to DEFAULT_PG_PORT and
    DEFAULT_BATCH_SIZE respectively on parse failure. Applies defaults for
    optional keys: pg_schema, pg_sslmode, adls_directory, and truncate_target.
    Raises ValueError listing all missing required keys, or if 'table' is
    empty after stripping whitespace.
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

    table = str(params["table"]).strip()
    if not table:
        raise ValueError("'table' must be a non-empty string.")

    try:
        pg_port = int(params.get("pg_port", DEFAULT_PG_PORT))
    except (ValueError, TypeError):
        pg_port = DEFAULT_PG_PORT

    batch_size = params.get("batch_size", DEFAULT_BATCH_SIZE)
    try:
        batch_size = int(batch_size)
        if batch_size < 1:
            batch_size = DEFAULT_BATCH_SIZE
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
        "adls_account_name": params["adls_account_name"],
        "adls_file_system": params["adls_file_system"],
        "adls_secret_name": params["adls_secret_name"],
        "adls_directory": params.get("adls_directory", DEFAULT_ADLS_DIRECTORY),
        "batch_size": batch_size,
        "truncate_target": bool(params.get("truncate_target", False)),
    }


# ============================================================================
# MAIN ACTIVITY FUNCTION
# ============================================================================


@app.activity_trigger(input_name="params")
def process_adls_to_postgres_activity(params: dict):
    """
    Azure Durable Functions activity that loads CSV data from ADLS into PostgreSQL.

    Orchestrates the full ETL pipeline for a single table:
    1. Validates and extracts parameters via validate_and_extract_params.
    2. Retrieves PostgreSQL and ADLS credentials from Azure Key Vault.
    3. Opens a PostgreSQL connection and validates ADLS connectivity.
    4. Introspects the target table for column types, primary keys, and
       server-default columns.
    5. Optionally truncates the target table when truncate_target is True.
    6. Resolves the ADLS directory path and lists all CSV files under the
       table subdirectory.
    7. Streams the parent CSV in batches of batch_size rows each, calling
       process_batch for each chunk. batch_size is passed through to both
       the parent stream and every child CSV read so all pipeline stages
       operate on the same window size.
    8. Returns a summary dict with status, row counts, batch count, child
       table counts, duration, and any error samples.

    Returns a dict with 'status' key set to 'success', 'completed_with_errors',
    or 'error', along with relevant metrics or an error message.
    """
    start_time = datetime.utcnow()
    conn = None

    try:
        params = validate_and_extract_params(params)

        pg_password = get_secret(params["key_vault_name"], params["pg_secret_name"])
        adls_account_key = get_secret(
            params["key_vault_name"], params["adls_secret_name"]
        )

        conn = get_postgres_connection(
            pg_host=params["pg_host"],
            pg_port=params["pg_port"],
            pg_database=params["pg_database"],
            pg_username=params["pg_username"],
            pg_password=pg_password,
            pg_sslmode=params["pg_sslmode"],
        )

        service_client = validate_adls_connection(
            params["adls_account_name"],
            params["adls_file_system"],
            adls_account_key,
        )

        validate_table_exists(conn, params["pg_schema"], params["table"])
        column_types, col_max_lengths = get_table_column_types(
            conn, params["pg_schema"], params["table"]
        )
        pk_columns = get_table_primary_keys(conn, params["pg_schema"], params["table"])
        table_columns = [c for c in column_types if ".__elem__" not in c]
        server_default_cols = get_server_default_columns(
            conn, params["pg_schema"], params["table"]
        )

        if not pk_columns:
            _LOG.warning(
                "Table '%s.%s' has no primary key. Plain INSERT will be used.",
                params["pg_schema"],
                params["table"],
            )

        if params["truncate_target"]:
            with conn.cursor() as cur:
                cur.execute(
                    f'TRUNCATE TABLE "{params["pg_schema"]}"."{params["table"]}" RESTART IDENTITY'
                )
            conn.commit()

        export_dir = (
            f"{params['adls_directory']}/{params['pg_database']}/{params['pg_schema']}"
            if params["adls_directory"]
            else f"{params['pg_database']}/{params['pg_schema']}"
        ).strip("/")

        table_dir = f"{export_dir}/{params['table']}"
        parent_full_path = f"{table_dir}/{params['table']}.csv"

        fs_client = service_client.get_file_system_client(params["adls_file_system"])
        all_paths = list(fs_client.get_paths(path=table_dir))
        csv_paths = [
            p.name
            for p in all_paths
            if (not p.is_directory) and p.name.endswith(".csv")
        ]

        if not csv_paths:
            return {
                "status": "success",
                "message": f"No CSV files found under '{table_dir}'.",
                "rows_processed": 0,
            }

        num_child_tables = sum(1 for p in csv_paths if p != parent_full_path)

        total_successful = 0
        total_failed = 0
        total_child_rows = 0
        all_error_samples: List[str] = []
        batch_num = 0
        total_parent_rows = 0

        for parent_chunk_df in stream_parent_csv_in_chunks(
            service_client,
            params["adls_file_system"],
            parent_full_path,
            chunk_rows=params["batch_size"],  # ← parent read uses batch_size
        ):
            if parent_chunk_df.empty:
                del parent_chunk_df
                continue

            batch_num += 1
            total_parent_rows += len(parent_chunk_df)

            if batch_num % PROGRESS_LOG_INTERVAL == 0 or batch_num == 1:
                _LOG.info(
                    "Batch %d: %d parent rows (%d total so far)",
                    batch_num,
                    len(parent_chunk_df),
                    total_parent_rows,
                )

            successful, failed, errors, child_rows = process_batch(
                batch_parents_df=parent_chunk_df,
                service_client=service_client,
                adls_file_system=params["adls_file_system"],
                csv_paths=csv_paths,
                table_dir=table_dir,
                table_name=params["table"],
                conn=conn,
                schema=params["pg_schema"],
                column_types=column_types,
                pk_columns=pk_columns,
                table_columns=table_columns,
                batch_size=params["batch_size"],  # ← child reads use the same batch_size
                server_default_cols=server_default_cols,
                col_max_lengths=col_max_lengths,
            )

            total_successful += successful
            total_failed += failed
            total_child_rows += child_rows
            all_error_samples.extend(errors[:BATCH_ERROR_SAMPLE_LIMIT])
            del parent_chunk_df
            gc.collect()

        if total_parent_rows == 0:
            return {
                "status": "success",
                "message": "Parent CSV is empty — nothing to load.",
                "rows_processed": 0,
            }

        duration = (datetime.utcnow() - start_time).total_seconds()
        return {
            "status": "success" if total_failed == 0 else "completed_with_errors",
            "table": f"{params['pg_schema']}.{params['table']}",
            "rows_processed": total_parent_rows,
            "batches": batch_num,
            "successful": total_successful,
            "failed": total_failed,
            "child_tables_joined": num_child_tables,
            "total_child_rows": total_child_rows,
            "pk_columns": pk_columns,
            "batch_size": params["batch_size"],
            "truncate_target": params["truncate_target"],
            "duration_seconds": round(duration, 2),
            "error_samples": all_error_samples[:ERROR_SAMPLE_LIMIT],
        }

    except Exception as e:
        _LOG.error("Activity failed: %s", str(e), exc_info=True)
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
def adls_to_postgres_orchestrator(context: df.DurableOrchestrationContext):
    """
    Durable Functions orchestrator that delegates to the activity function.

    Reads the input payload from the orchestration context and invokes the
    process_adls_to_postgres_activity activity, yielding control until it
    completes. Returns the activity result directly to the caller.
    """
    params = context.get_input()
    result = yield context.call_activity("process_adls_to_postgres_activity", params)
    return result


@app.route(route="ADLS_to_Postgres_V1", methods=["POST"])
@app.durable_client_input(client_name="client")
async def adls_to_postgres_http_start(
    req: func.HttpRequest, client
) -> func.HttpResponse:
    """
    HTTP trigger that starts a new orchestration instance for the ADLS-to-PostgreSQL pipeline.

    Parses the JSON request body and passes it as input to the
    adls_to_postgres_orchestrator orchestration. Returns a 400 response on
    invalid JSON, the Durable Functions check-status response on success, or a
    500 JSON error response if starting the orchestration fails.
    """
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse("Invalid JSON body", status_code=400)
    try:
        instance_id = await client.start_new(
            "adls_to_postgres_orchestrator", None, body
        )
        return client.create_check_status_response(req, instance_id)
    except Exception as e:
        _LOG.error("HTTP start failed: %s", str(e), exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )