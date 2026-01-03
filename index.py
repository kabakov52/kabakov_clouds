import os
import json
import uuid
import datetime
from typing import Any, Dict, Optional

import ydb
import ydb.iam


def _json_default(o):
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    return str(o)


# --- YDB connection (reuse across invocations) ---
YDB_ENDPOINT = os.environ.get("YDB_ENDPOINT")
YDB_DATABASE = os.environ.get("YDB_DATABASE")

if not YDB_ENDPOINT or not YDB_DATABASE:
    raise RuntimeError("Missing env vars YDB_ENDPOINT / YDB_DATABASE")

_driver = None
_pool = None


def _get_pool() -> ydb.SessionPool:
    global _driver, _pool
    if _pool is not None:
        return _pool

    credentials = ydb.iam.MetadataUrlCredentials()

    _driver = ydb.Driver(
        endpoint=YDB_ENDPOINT,
        database=YDB_DATABASE,
        credentials=credentials,
    )
    _driver.wait(fail_fast=True, timeout=10)

    _pool = ydb.SessionPool(_driver, size=10)
    return _pool


def _now_dt() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(microsecond=0)


def _resp(status: int, body: Any = None, extra_headers: Optional[Dict[str, str]] = None):
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
        "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
    }
    if extra_headers:
        headers.update(extra_headers)

    return {
        "statusCode": status,
        "headers": headers,
        "body": "" if body is None else json.dumps(body, ensure_ascii=False, default=_json_default),
    }


def _parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
    body = event.get("body")
    if not body:
        return {}
    if isinstance(body, (dict, list)):
        return body
    return json.loads(body)


def _path(event: Dict[str, Any]) -> str:
    return event.get("path") or "/"


def _method(event: Dict[str, Any]) -> str:
    return (event.get("httpMethod") or "GET").upper()


def _path_params(event: Dict[str, Any]) -> Dict[str, str]:
    return event.get("pathParameters") or {}


def _query_params(event: Dict[str, Any]) -> Dict[str, str]:
    return event.get("queryStringParameters") or {}


def _exec(query: str, params: Optional[Dict[str, Any]] = None):
    pool = _get_pool()

    def _op(session: ydb.Session):
        prepared = session.prepare(query)
        tx = session.transaction(ydb.SerializableReadWrite()).begin()
        result_sets = tx.execute(prepared, parameters=params or {}, commit_tx=True)
        return result_sets

    return pool.retry_operation_sync(_op)


def _select_all(query: str, params: Optional[Dict[str, Any]] = None):
    rs = _exec(query, params=params)[0]
    return [dict(r) for r in rs.rows]


def _parse_date_yyyy_mm_dd(s: str) -> datetime.date:
    try:
        return datetime.date.fromisoformat(s)
    except Exception:
        raise ValueError("Invalid date format. Expected YYYY-MM-DD")


def _id_from_path(path: str, prefix: str) -> Optional[str]:
    """
    Extract ID from paths like '/properties/<id>' reliably.
    prefix must be like '/properties/' (with trailing slash).
    """
    if not path.startswith(prefix):
        return None
    parts = path.strip("/").split("/")
    # e.g. ['properties', '<id>']
    if len(parts) < 2 or not parts[1]:
        return None
    return parts[1]


def _get_id(event: Dict[str, Any], name: str, prefix: str) -> Optional[str]:
    """
    Universal ID extractor:
    1) pathParameters[name]
    2) queryStringParameters[name]
    3) parse from path '/prefix/<id>'
    Works even if event['path'] is '/prefix/{id}' (then uses pathParameters).
    """
    path = _path(event)
    pp = _path_params(event)
    qp = _query_params(event)

    v = pp.get(name) or qp.get(name)
    if v:
        return v

    v = _id_from_path(path, prefix)
    if v and v != "{id}":
        return v

    # If path is template like '/properties/{id}'
    if path == prefix.rstrip("/") + "/{id}":
        return pp.get(name) or qp.get(name)

    return None


# --- Handlers ---
def handle_health():
    return _resp(200, {"ok": True, "ts_utc": _now_dt().isoformat() + "Z"})


# Properties
def properties_list():
    rows = _select_all(
        """
        SELECT property_id, address, status, notes, created_at
        FROM properties
        ORDER BY created_at DESC;
        """
    )
    return _resp(200, rows)


def properties_create(payload: Dict[str, Any]):
    pid = str(uuid.uuid4())
    address = payload.get("address", "").strip()
    status = str(payload.get("status", "AVAILABLE")).strip().upper()
    notes = payload.get("notes", "")

    if not address:
        return _resp(400, {"error": "address is required"})

    _exec(
        """
        DECLARE $property_id AS Utf8;
        DECLARE $address AS Utf8;
        DECLARE $status AS Utf8;
        DECLARE $notes AS Utf8;

        UPSERT INTO properties (property_id, address, status, notes, created_at)
        VALUES ($property_id, $address, $status, $notes, CurrentUtcDatetime());
        """,
        params={
            "$property_id": pid,
            "$address": address,
            "$status": status,
            "$notes": notes,
        },
    )
    return _resp(201, {"property_id": pid})


def properties_get(property_id: str):
    rows = _select_all(
        """
        DECLARE $id AS Utf8;

        SELECT property_id, address, status, notes, created_at
        FROM properties
        WHERE property_id = $id;
        """,
        params={"$id": property_id},
    )
    if not rows:
        return _resp(404, {"error": "not found"})
    return _resp(200, rows[0])


def properties_update(property_id: str, payload: Dict[str, Any]):
    cur = _select_all(
        """
        DECLARE $id AS Utf8;

        SELECT property_id, address, status, notes, created_at
        FROM properties
        WHERE property_id = $id;
        """,
        params={"$id": property_id},
    )
    if not cur:
        return _resp(404, {"error": "not found"})

    address = payload.get("address", cur[0]["address"])
    status = payload.get("status", cur[0]["status"])
    notes = payload.get("notes", cur[0]["notes"])
    created_at = cur[0]["created_at"]  # keep original

    _exec(
        """
        DECLARE $property_id AS Utf8;
        DECLARE $address AS Utf8;
        DECLARE $status AS Utf8;
        DECLARE $notes AS Utf8;
        DECLARE $created_at AS Datetime;

        UPSERT INTO properties (property_id, address, status, notes, created_at)
        VALUES ($property_id, $address, $status, $notes, $created_at);
        """,
        params={
            "$property_id": property_id,
            "$address": str(address),
            "$status": str(status).strip().upper(),
            "$notes": str(notes),
            "$created_at": created_at,
        },
    )
    return _resp(200, {"ok": True})


def properties_delete(property_id: str):
    _exec(
        """
        DECLARE $id AS Utf8;
        DELETE FROM properties WHERE property_id = $id;
        """,
        params={"$id": property_id},
    )
    return _resp(200, {"ok": True, "deleted_id": property_id})


# Tenants
def tenants_list():
    rows = _select_all(
        """
        SELECT tenant_id, full_name, phone, email, created_at
        FROM tenants
        ORDER BY created_at DESC;
        """
    )
    return _resp(200, rows)


def tenants_create(payload: Dict[str, Any]):
    tid = str(uuid.uuid4())
    full_name = payload.get("full_name", "").strip()
    phone = payload.get("phone", "").strip()
    email = payload.get("email", "").strip()

    if not full_name:
        return _resp(400, {"error": "full_name is required"})

    _exec(
        """
        DECLARE $tenant_id AS Utf8;
        DECLARE $full_name AS Utf8;
        DECLARE $phone AS Utf8;
        DECLARE $email AS Utf8;

        UPSERT INTO tenants (tenant_id, full_name, phone, email, created_at)
        VALUES ($tenant_id, $full_name, $phone, $email, CurrentUtcDatetime());
        """,
        params={
            "$tenant_id": tid,
            "$full_name": full_name,
            "$phone": phone,
            "$email": email,
        },
    )
    return _resp(201, {"tenant_id": tid})


def tenants_get(tenant_id: str):
    rows = _select_all(
        """
        DECLARE $id AS Utf8;

        SELECT tenant_id, full_name, phone, email, created_at
        FROM tenants
        WHERE tenant_id = $id;
        """,
        params={"$id": tenant_id},
    )
    if not rows:
        return _resp(404, {"error": "not found"})
    return _resp(200, rows[0])


def tenants_update(tenant_id: str, payload: Dict[str, Any]):
    cur = _select_all(
        """
        DECLARE $id AS Utf8;

        SELECT tenant_id, full_name, phone, email, created_at
        FROM tenants
        WHERE tenant_id = $id;
        """,
        params={"$id": tenant_id},
    )
    if not cur:
        return _resp(404, {"error": "not found"})

    full_name = payload.get("full_name", cur[0]["full_name"])
    phone = payload.get("phone", cur[0]["phone"])
    email = payload.get("email", cur[0]["email"])
    created_at = cur[0]["created_at"]

    _exec(
        """
        DECLARE $tenant_id AS Utf8;
        DECLARE $full_name AS Utf8;
        DECLARE $phone AS Utf8;
        DECLARE $email AS Utf8;
        DECLARE $created_at AS Datetime;

        UPSERT INTO tenants (tenant_id, full_name, phone, email, created_at)
        VALUES ($tenant_id, $full_name, $phone, $email, $created_at);
        """,
        params={
            "$tenant_id": tenant_id,
            "$full_name": str(full_name),
            "$phone": str(phone),
            "$email": str(email),
            "$created_at": created_at,
        },
    )
    return _resp(200, {"ok": True})


def tenants_delete(tenant_id: str):
    _exec(
        """
        DECLARE $id AS Utf8;
        DELETE FROM tenants WHERE tenant_id = $id;
        """,
        params={"$id": tenant_id},
    )
    return _resp(200, {"ok": True, "deleted_id": tenant_id})


# Leases
def leases_list():
    rows = _select_all(
        """
        SELECT lease_id, property_id, tenant_id, start_date, end_date, created_at
        FROM leases
        ORDER BY created_at DESC;
        """
    )
    return _resp(200, rows)


def leases_create(payload: Dict[str, Any]):
    lease_id = str(uuid.uuid4())
    property_id = payload.get("property_id", "").strip()
    tenant_id = payload.get("tenant_id", "").strip()
    start_date_s = payload.get("start_date", "").strip()
    end_date_s = (payload.get("end_date") or "").strip()

    if not property_id or not tenant_id or not start_date_s:
        return _resp(400, {"error": "property_id, tenant_id, start_date are required"})

    try:
        start_date = _parse_date_yyyy_mm_dd(start_date_s)
        end_date = _parse_date_yyyy_mm_dd(end_date_s) if end_date_s else None
    except ValueError as e:
        return _resp(400, {"error": str(e)})

    _exec(
        """
        DECLARE $lease_id AS Utf8;
        DECLARE $property_id AS Utf8;
        DECLARE $tenant_id AS Utf8;
        DECLARE $start_date AS Date;
        DECLARE $end_date AS Date?;

        UPSERT INTO leases (lease_id, property_id, tenant_id, start_date, end_date, created_at)
        VALUES ($lease_id, $property_id, $tenant_id, $start_date, $end_date, CurrentUtcDatetime());
        """,
        params={
            "$lease_id": lease_id,
            "$property_id": property_id,
            "$tenant_id": tenant_id,
            "$start_date": start_date,
            "$end_date": end_date,
        },
    )

    # set property status to RENTED
    _exec(
        """
        DECLARE $id AS Utf8;
        DECLARE $status AS Utf8;

        UPSERT INTO properties (property_id, status)
        VALUES ($id, $status);
        """,
        params={"$id": property_id, "$status": "RENTED"},
    )

    return _resp(201, {"lease_id": lease_id})


def leases_delete(lease_id: str):
    _exec(
        """
        DECLARE $id AS Utf8;
        DELETE FROM leases WHERE lease_id = $id;
        """,
        params={"$id": lease_id},
    )
    return _resp(200, {"ok": True, "deleted_id": lease_id})


def handler(event, context):
    if _method(event) == "OPTIONS":
        return _resp(200, {"ok": True})

    path = _path(event)
    method = _method(event)

    try:
        if path == "/health":
            return handle_health()

        # --- properties collection ---
        if path == "/properties" and method == "GET":
            return properties_list()
        if path == "/properties" and method == "POST":
            return properties_create(_parse_body(event))

        # --- properties item ---
        prop_id = _get_id(event, "id", "/properties/")
        if prop_id is not None:
            if method == "GET":
                return properties_get(prop_id)
            if method == "PUT":
                return properties_update(prop_id, _parse_body(event))
            if method == "DELETE":
                return properties_delete(prop_id)

        # --- tenants collection ---
        if path == "/tenants" and method == "GET":
            return tenants_list()
        if path == "/tenants" and method == "POST":
            return tenants_create(_parse_body(event))

        # --- tenants item ---
        tenant_id = _get_id(event, "id", "/tenants/")
        if tenant_id is not None:
            if method == "GET":
                return tenants_get(tenant_id)
            if method == "PUT":
                return tenants_update(tenant_id, _parse_body(event))
            if method == "DELETE":
                return tenants_delete(tenant_id)

        # --- leases collection ---
        if path == "/leases" and method == "GET":
            return leases_list()
        if path == "/leases" and method == "POST":
            return leases_create(_parse_body(event))

        # --- leases item ---
        lease_id = _get_id(event, "id", "/leases/")
        if lease_id is not None:
            if method == "DELETE":
                return leases_delete(lease_id)

        return _resp(404, {"error": "unknown route", "path": path, "method": method})

    except Exception as e:
        return _resp(500, {"error": "internal error", "details": str(e)})
