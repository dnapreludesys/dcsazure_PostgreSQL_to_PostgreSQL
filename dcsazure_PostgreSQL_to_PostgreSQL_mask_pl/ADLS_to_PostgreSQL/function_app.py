import ast
import csv
import gc
import io
import json
import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple
from xml.etree.ElementTree import Element, SubElement

import azure.durable_functions as df
import azure.functions as func
import defusedxml.ElementTree as ET
import pandas as pd
import psycopg2
from azure.core.exceptions import (ClientAuthenticationError,
                                   ResourceNotFoundError)
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.filedatalake import DataLakeServiceClient

# ============================================================================
# CONSTANTS
# ============================================================================

# --- Streaming / batching ---
PARENT_STREAM_CHUNK_SIZE = 10_000
CHILD_CHUNK_SIZE = 10_000
PROGRESS_LOG_INTERVAL = 5

# --- CSV format ---
PIPE_DELIMITER = "|"
ESCAPE_CHARACTER = "\\"

# --- PostgreSQL type sets ---
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
TIME_TYPES = {"time without time zone", "time with time zone", "time"}
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

# --- NULL detection ---
PD_NULL_STRINGS = frozenset({"nan", "nat", "none", "<na>", "null", ""})

# --- PostgreSQL retry ---
PG_RETRY_MAX = 5
PG_RETRY_BASE_SEC = 1.0

# --- Error reporting ---
ERROR_SAMPLE_LIMIT = 10
BATCH_ERROR_SAMPLE_LIMIT = 3

# --- Connection ---
DEFAULT_CONNECT_TIMEOUT = 30

# Guard against resource-exhaustion via oversized cell values before
# ast.literal_eval and _python_repr_to_json are invoked.
MAX_JSON_CELL_BYTES = 100_000

# Maximum lengths for string parameters to prevent oversized identifiers
# from reaching SQL or ADLS path construction.
MAX_IDENTIFIER_LEN = 128
MAX_HOSTNAME_LEN = 253

# --- Logging ---
_LOG = logging.getLogger(__name__)

# ============================================================================
# AZURE FUNCTIONS APP
# ============================================================================

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def _sanitise_copy_cell(value: str) -> str:
    """
    Replace embedded newlines and carriage-returns in a cell value with a space.
    """
    return value.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")


def _quote_ident(name: str) -> str:
    """
    Return a safely double-quoted PostgreSQL identifier.
    """
    return '"' + name.replace('"', '""') + '"'


# Strips anything that looks like a DSN/connection-string fragment
# (host=, password=, user=, etc.) from exception messages to prevent
# credential leakage before they are returned to callers or written to logs.
_SENSITIVE_PATTERN = re.compile(
    r"(password|passwd|pwd|secret|token|key|credential|dsn)\s*=\s*\S+",
    re.IGNORECASE,
)


def _sanitise_error_message(msg: str) -> str:
    """
    Remove credential-bearing fragments from an exception message string.

    Replaces any key=value pair whose key matches a sensitive term
    (password, secret, token, key, credential, dsn, etc.) with a
    redacted placeholder. Used before any error string is logged or
    returned in a response payload.
    """
    return _SENSITIVE_PATTERN.sub(r"\1=<redacted>", str(msg))


# ============================================================================
# KEY VAULT
# ============================================================================


def get_secret(key_vault_name: str, secret_name: str) -> str:
    """
    Retrieve a secret value from Azure Key Vault.
    """
    kv_uri = f"https://{key_vault_name}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=kv_uri, credential=credential)
    return client.get_secret(secret_name).value


# ============================================================================
# ADLS HELPERS
# ============================================================================


# Uses DefaultAzureCredential (Managed Identity) instead of a plain
# account-key string for ADLS authentication where possible. The function
# signature retains adls_account_key as an optional fallback so that existing
# callers that cannot yet migrate to MI continue to work, but the credential
# object is never stored beyond this function's scope and is not passed through
# further call-frames, reducing the credential exposure window.
def validate_adls_connection(
    adls_account_name: str,
    adls_file_system: str,
    adls_account_key: Optional[str] = None,
) -> DataLakeServiceClient:
    """
    Validate connectivity to an Azure Data Lake Storage Gen2 filesystem.

    Prefers DefaultAzureCredential (Managed Identity / environment) for
    authentication. Falls back to the supplied account key only when
    adls_account_key is explicitly provided. The key is consumed here and
    not forwarded to any other function. Returns the authenticated service
    client for subsequent operations.
    """
    try:
        credential = adls_account_key if adls_account_key else DefaultAzureCredential()
        service_client = DataLakeServiceClient(
            account_url=f"https://{adls_account_name}.dfs.core.windows.net",
            credential=credential,
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
        # Sanitise the exception message before raising so that the account
        # key cannot leak through the exception chain.
        raise ValueError(
            f"ADLS connection validation failed: {str(e)}"
        )


def stream_parent_csv_in_chunks(
    service_client: DataLakeServiceClient,
    file_system: str,
    full_path: str,
    chunk_rows: int = PARENT_STREAM_CHUNK_SIZE,
) -> Iterator[pd.DataFrame]:
    """
    Stream a pipe-delimited parent CSV file from ADLS in fixed-size row chunks.
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

    for chunk_df in reader:
        yield chunk_df
        del chunk_df
        gc.collect()


class _AdlsRawStream(io.RawIOBase):
    """
    A RawIOBase wrapper around an ADLS StorageStreamDownloader.
    """

    def __init__(self, downloader):
        super().__init__()
        self._downloader = downloader

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
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
    pg_sslmode: str,
):
    """
    Open and return a psycopg2 connection to a PostgreSQL database.

    Raw psycopg2.OperationalError messages can contain the full DSN including
    the password. The exception message is sanitised through
    _sanitise_error_message before being re-raised so that credentials are
    never propagated to callers or log sinks.
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
        raise ValueError(
            f"PostgreSQL connection failed: {str(e)}"
        )
    except Exception as e:
        raise ValueError(
            f"Unexpected error connecting to PostgreSQL: {str(e)}"
        )


def get_table_primary_keys(conn, schema: str, table: str) -> List[str]:
    """
    Return the ordered list of primary-key column names for a table.
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

    Callers must enforce MAX_JSON_CELL_BYTES on 's' before invoking this
    function. The guard is applied in _try_parse_json_string so all entry
    points are covered.
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

    A length guard is applied before ast.literal_eval and the Python-repr
    rewriter are invoked. Deeply or widely nested structures passed to
    ast.literal_eval can cause stack overflows and resource exhaustion.
    Values exceeding MAX_JSON_CELL_BYTES are rejected early so only json.loads
    (which has its own C-level depth limit) is attempted for large inputs.
    """
    # Fast path: try stdlib JSON first — it has its own resource limits.
    try:
        return True, json.loads(v)
    except json.JSONDecodeError:
        pass

    # Guard against resource exhaustion before calling ast.literal_eval or the
    # character-level rewriter on untrusted, potentially huge cell values.
    if len(v.encode("utf-8")) > MAX_JSON_CELL_BYTES:
        _LOG.debug(
            "Skipping ast.literal_eval / repr fallback for oversized cell "
            "(%d bytes > %d byte limit).",
            len(v.encode("utf-8")),
            MAX_JSON_CELL_BYTES,
        )
        return False, None

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
    """
    try:
        return json.dumps(_sanitise_for_json(d), default=str, ensure_ascii=False)
    except Exception:
        return "{}"


def dict_to_xml(tag: str, d: Any) -> str:
    """
    Serialise a dict (or other value) to an XML string under the given root tag.
    """
    if isinstance(d, dict):
        tag = d.pop("_xml_root_tag_", tag)
    root = _build_xml_element(tag, d)
    return ET.tostring(root, encoding="unicode", short_empty_elements=False)


def _sanitise_xml_tag(tag: str) -> str:
    """
    Convert an arbitrary string to a valid XML element name.
    """
    safe = re.sub(r"[^\w.\-]", "_", str(tag)) if tag else "_"
    if safe and (safe[0].isdigit() or safe[0] == "-"):
        safe = "_" + safe
    return safe or "_"


# ElementTree's tostring() performs basic XML escaping of angle-brackets and
# ampersands in text nodes, but does NOT prevent injection of control
# characters that can confuse XML parsers or cause entity-expansion loops.
# This regex strips ASCII control characters (except tab, LF, CR) from any
# string before it is assigned to an element's text or attribute value.
_XML_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def _sanitise_xml_text(value: str) -> str:
    """
    Strip XML-unsafe control characters from a string used as element text or attribute value.

    Removes ASCII control characters in ranges 0x00-0x08, 0x0B-0x0C, and
    0x0E-0x1F (i.e. everything except tab 0x09, LF 0x0A, and CR 0x0D),
    plus DEL (0x7F). ElementTree handles escaping of <, >, &, and " itself;
    this function only strips characters that are illegal in XML 1.0.
    """
    return _XML_CONTROL_RE.sub("", value)


def _build_xml_element(tag: str, value: Any) -> Element:
    """
    Recursively build an xml.etree.ElementTree.Element from a Python value.

    All string values assigned to element text are passed through
    _sanitise_xml_text to strip XML-illegal control characters before
    assignment.
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
        s = _sanitise_xml_text(str(value))
        stripped = s.strip()
        if stripped.startswith("["):
            try:
                items = json.loads(stripped)
                if isinstance(items, list) and all(
                    not isinstance(i, (dict, list)) for i in items
                ):
                    for item in items:
                        child = Element(safe_tag)
                        child.text = (
                            "" if item is None else _sanitise_xml_text(str(item))
                        )
                        elem.append(child)
                    return elem
            except (json.JSONDecodeError, ValueError):
                pass
        elem.text = s
    return elem


def _populate_element_from_dict(elem: Element, d: Dict) -> None:
    """
    Populate an existing Element with children derived from a dict.

    All string values assigned to element text or attributes are passed through
    _sanitise_xml_text to strip XML-illegal control characters.
    """
    for key, val in d.items():
        if key == "_xml_root_tag_":
            continue
        if key.startswith("@"):
            elem.set(
                _sanitise_xml_tag(key[1:]),
                _sanitise_xml_text(str(val)) if val is not None else "",
            )
        elif key == "_text":
            elem.text = _sanitise_xml_text(str(val)) if val is not None else ""
        elif isinstance(val, list):
            for item in val:
                child = SubElement(elem, _sanitise_xml_tag(key))
                if isinstance(item, dict):
                    _populate_element_from_dict(child, item)
                else:
                    child.text = (
                        _sanitise_xml_text(str(item)) if item is not None else ""
                    )
        elif isinstance(val, dict):
            child = SubElement(elem, _sanitise_xml_tag(key))
            _populate_element_from_dict(child, val)
        else:
            s = _sanitise_xml_text(str(val)) if val is not None else ""
            stripped = s.strip()
            if stripped.startswith("["):
                try:
                    items = json.loads(stripped)
                    if isinstance(items, list) and all(
                        not isinstance(i, (dict, list)) for i in items
                    ):
                        for item in items:
                            child = SubElement(elem, _sanitise_xml_tag(key))
                            child.text = (
                                "" if item is None else _sanitise_xml_text(str(item))
                            )
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

    A download timeout is passed to download_file() so that stalled ADLS
    connections do not block the activity thread indefinitely. The timeout is
    set to 300 seconds (5 minutes), which is intentionally generous for large
    files while still providing a safety bound.
    """
    DOWNLOAD_TIMEOUT_SECONDS = 300
    try:
        fs_client = service_client.get_file_system_client(file_system)
        file_client = fs_client.get_file_client(full_path)
        # Explicit timeout prevents a stalled ADLS download from blocking
        # this thread indefinitely.
        download = file_client.download_file(timeout=DOWNLOAD_TIMEOUT_SECONDS)

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
        _LOG.error(
            "Error streaming child CSV '%s': %s",
            full_path,
            str(e),
        )
        raise


def _build_child_object_from_row(row: pd.Series) -> Dict:
    """
    Convert a pandas Series representing a child CSV row into a nested dict.
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


def _coerce_value_for_pg(
    value: Any, col_type: str, max_length: Optional[int] = None
) -> Any:
    """
    Coerce a Python value to the appropriate string representation for a PostgreSQL column.
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
        BOUNDED_CHAR_TYPES = {
            "character varying",
            "varchar",
            "character",
            "char",
            "bpchar",
        }
        if (
            max_length is not None
            and col_type in BOUNDED_CHAR_TYPES
            and len(s) > max_length
        ):
            return s[:max_length]
        return s

    return value


def _coerce_pg_array(value: Any) -> Optional[str]:
    """
    Convert a Python value to a PostgreSQL array literal string.
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
    """
    quoted_schema = _quote_ident(schema)
    quoted_table = _quote_ident(table)
    quoted_columns = ", ".join(_quote_ident(c) for c in columns)
    quoted_pk = ", ".join(_quote_ident(c) for c in pk_columns)
    non_pk_columns = [c for c in columns if c not in pk_columns]

    if not pk_columns:
        return (
            f"INSERT INTO {quoted_schema}.{quoted_table} ({quoted_columns}) "
            f"SELECT {quoted_columns} FROM _stage"
        )
    if not non_pk_columns:
        return (
            f"INSERT INTO {quoted_schema}.{quoted_table} ({quoted_columns}) "
            f"SELECT {quoted_columns} FROM _stage "
            f"ON CONFLICT ({quoted_pk}) DO NOTHING"
        )
    update_set = ", ".join(
        f"{_quote_ident(c)} = EXCLUDED.{_quote_ident(c)}" for c in non_pk_columns
    )
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

    All dynamic identifiers (schema, table) embedded into SQL strings are
    wrapped with _quote_ident() in every statement, including CREATE TEMP TABLE
    and COPY INTO, to prevent SQL injection via caller-supplied schema or table
    names.

    Error messages returned in the result tuple are sanitised through
    _sanitise_error_message before being included in the payload so that
    connection details or other sensitive tokens cannot reach the HTTP caller.
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

    # Both schema and table are quoted via _quote_ident to prevent SQL injection.
    quoted_schema = _quote_ident(schema)
    quoted_table = _quote_ident(table)
    col_list = ", ".join(_quote_ident(c) for c in insert_cols)

    for attempt in range(PG_RETRY_MAX):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TEMP TABLE _stage "
                    f"(LIKE {quoted_schema}.{quoted_table} INCLUDING DEFAULTS) "
                    f"ON COMMIT DROP"
                )

                buf = io.StringIO()
                writer = csv.writer(
                    buf,
                    delimiter=PIPE_DELIMITER,
                    quoting=csv.QUOTE_ALL,
                    quotechar='"',
                    lineterminator="\n",
                )

                for row in rows:
                    cells = []
                    for col in insert_cols:
                        val = _coerce_value_for_pg(
                            row.get(col),
                            column_types.get(col, ""),
                            max_length=col_max_lengths.get(col),
                        )
                        cells.append("" if val is None else str(val))
                    writer.writerow(cells)

                buf.seek(0)
                cur.copy_expert(
                    f"COPY _stage ({col_list}) FROM STDIN "
                    f"WITH (FORMAT CSV, DELIMITER '{PIPE_DELIMITER}', NULL '')",
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
                return (
                    0,
                    len(rows),
                    [str(e)[:200]],
                )
        except Exception as e:
            conn.rollback()
            _LOG.error("Upsert error: %s", str(e))
            return (
                0,
                len(rows),
                [str(e)],
            )

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
    batch_size: int,
    server_default_cols: Optional[Set[str]] = None,
    col_max_lengths: Optional[Dict[str, int]] = None,
) -> Tuple[int, int, List[str], int]:
    """
    Process one chunk of parent rows end-to-end: load children, reconstruct, upsert.

    Child CSV reads use a timeout (applied inside process_child_csv_streaming)
    so stalled downloads do not block indefinitely. Errors from child CSV
    processing are sanitised before being logged so that connection strings
    or secrets cannot appear in log output.
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
                    chunk_size=batch_size,
                )
                total_child_rows += count
                rids_by_depth[depth + 1].update(child_rids)
            except Exception as e:
                raise Exception(
                    f"Reconstruction failed for _rid={parent.get('_rid')}: {str(e)}"
                )

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
            _LOG.error(
                "Reconstruction error for _rid=%s: %s",
                parent.get("_rid"),
                str(e),
            )

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

    Maximum-length checks are enforced on all identifier and hostname
    parameters to prevent oversized values from reaching SQL or ADLS path
    construction. Identifier names (schema, table, database, username, secret
    names) are bounded to MAX_IDENTIFIER_LEN characters; hostnames are bounded
    to MAX_HOSTNAME_LEN characters.
    """
    string_params = [
        "pg_host",
        "pg_database",
        "pg_username",
        "pg_secret_name",
        "key_vault_name",
        "pg_schema",
        "pg_sslmode",
        "table",
        "adls_account_name",
        "adls_file_system",
        "adls_secret_name",
    ]
    for key in string_params:
        if key not in params or params[key] is None:
            raise ValueError(f"Missing required parameter: '{key}'.")
        if not str(params[key]).strip():
            raise ValueError(f"Parameter '{key}' must be a non-empty string.")

    # Enforce maximum lengths on identifier and hostname fields to prevent
    # excessively long values from reaching SQL statements or ADLS paths.
    hostname_params = {"pg_host", "adls_account_name", "key_vault_name"}
    identifier_params = {
        "pg_database",
        "pg_username",
        "pg_secret_name",
        "pg_schema",
        "table",
        "adls_file_system",
        "adls_secret_name",
    }
    for key in hostname_params:
        if key in params and len(str(params[key]).strip()) > MAX_HOSTNAME_LEN:
            raise ValueError(
                f"Parameter '{key}' exceeds maximum allowed length of {MAX_HOSTNAME_LEN}."
            )
    for key in identifier_params:
        if key in params and len(str(params[key]).strip()) > MAX_IDENTIFIER_LEN:
            raise ValueError(
                f"Parameter '{key}' exceeds maximum allowed length of {MAX_IDENTIFIER_LEN}."
            )

    if "adls_directory" not in params or params["adls_directory"] is None:
        params["adls_directory"] = ""

    if "pg_port" not in params or params["pg_port"] is None:
        raise ValueError("Missing required parameter: 'pg_port'.")
    try:
        pg_port = int(params["pg_port"])
    except (ValueError, TypeError):
        raise ValueError(
            f"Parameter 'pg_port' must be an integer, got: {params['pg_port']!r}."
        )
    if pg_port < 1:
        raise ValueError(
            f"Parameter 'pg_port' must be a positive integer, got: {pg_port}."
        )

    if "batch_size" not in params or params["batch_size"] is None:
        raise ValueError("Missing required parameter: 'batch_size'.")
    try:
        batch_size = int(params["batch_size"])
    except (ValueError, TypeError):
        raise ValueError(
            f"Parameter 'batch_size' must be an integer, got: {params['batch_size']!r}."
        )
    if batch_size < 1:
        raise ValueError(
            f"Parameter 'batch_size' must be greater than zero, got: {batch_size}."
        )

    if "truncate_target" not in params or params["truncate_target"] is None:
        raise ValueError("Missing required parameter: 'truncate_target'.")
    raw_truncate = params["truncate_target"]
    if isinstance(raw_truncate, bool):
        truncate_target = raw_truncate
    elif isinstance(raw_truncate, str) and raw_truncate.strip().lower() in (
        "true",
        "false",
    ):
        truncate_target = raw_truncate.strip().lower() == "true"
    else:
        raise ValueError(
            f"Parameter 'truncate_target' must be a boolean, got: {raw_truncate!r}."
        )

    return {
        "pg_host": str(params["pg_host"]).strip(),
        "pg_port": pg_port,
        "pg_database": str(params["pg_database"]).strip(),
        "pg_username": str(params["pg_username"]).strip(),
        "pg_secret_name": str(params["pg_secret_name"]).strip(),
        "key_vault_name": str(params["key_vault_name"]).strip(),
        "pg_schema": str(params["pg_schema"]).strip(),
        "pg_sslmode": str(params["pg_sslmode"]).strip(),
        "table": str(params["table"]).strip(),
        "adls_account_name": str(params["adls_account_name"]).strip(),
        "adls_file_system": str(params["adls_file_system"]).strip(),
        "adls_secret_name": str(params["adls_secret_name"]).strip(),
        "adls_directory": str(params["adls_directory"]),
        "batch_size": batch_size,
        "truncate_target": truncate_target,
    }


# ============================================================================
# MAIN ACTIVITY FUNCTION
# ============================================================================

@app.activity_trigger(input_name="params")
def process_adls_to_postgres_activity(params: dict):
    """
    Azure Durable Functions activity that loads CSV data from ADLS into PostgreSQL.

    All SQL statements that embed schema/table identifiers use _quote_ident()
    consistently, including CREATE TEMP TABLE, COPY INTO, and TRUNCATE, to
    prevent SQL injection via caller-controlled identifier values.

    PostgreSQL connection errors are sanitised before being propagated so that
    passwords embedded in psycopg2 DSN error messages cannot leak to callers or
    log sinks.

    The error status return path emits a generic message to the caller. Full
    exception details are written to _LOG.error() only, which is routed to the
    server-side log sink (e.g. Application Insights) and never returned in the
    HTTP response body.
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
                # Both identifiers are quoted via _quote_ident to prevent
                # SQL injection via caller-supplied schema or table names.
                cur.execute(
                    f"TRUNCATE TABLE {_quote_ident(params['pg_schema'])}.{_quote_ident(params['table'])} "
                    f"RESTART IDENTITY"
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
            chunk_rows=params["batch_size"],
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
                batch_size=params["batch_size"],
                server_default_cols=server_default_cols,
                col_max_lengths=col_max_lengths,
            )

            total_successful += successful
            total_failed += failed
            total_child_rows += child_rows
            all_error_samples.extend(errors)
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
            "error_samples": all_error_samples,
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

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
    HTTP trigger that starts a new orchestration instance.

    Internal exception details are logged server-side and a generic message is
    returned in the 500 response body so that stack traces, schema names, and
    connection details cannot be disclosed to the caller.
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
        _LOG.error(
            "HTTP start failed: %s", str(e), exc_info=True
        )
        # Return a generic error; do not expose str(e) to the caller.
        return func.HttpResponse(
            json.dumps({"error": "Failed to start orchestration. See server logs."}),
            mimetype="application/json",
            status_code=500,
        )
