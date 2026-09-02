"""The vendor's HTTP API.

Base path ``/v1``.  Bearer auth, JSON, opaque cursor pagination.

``since``/``until`` filter on valid time.  ``as_of`` filters on knowledge time
and defaults to the current simulated moment, so a plain read gives you what
this system knows right now and a read with ``as_of`` gives you what it knew
then.

Lab-control endpoints live under ``/_lab/`` and are not part of the simulated
vendor surface.
"""

from __future__ import annotations

import base64
import datetime as dt
import gzip
import hashlib
import hmac
import json
import random
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse, Response

from lab import config as cfgmod
from lab import drop as dropmod
from lab.clock import Clock
from lab.faults.effects import Effects, RenderPolicy
from lab.world import RENDER_ALLOWLIST, Store, check_payload

MAX_LIMIT = 1000
DEFAULT_LIMIT = 200
CAPPED_LIMIT = 500
TOKEN_TTL_SIM_HOURS = 6.0

ENDPOINT_TABLES = {
    "orders": "orders",
    "customers": "customers",
    "events": "events",
    "labels": "labels",
}

# Which spelling of "no value" each endpoint happens to use once the service is
# under load.  Fixed per endpoint, inconsistent across them.
_NULL_STYLE = {"orders": "", "customers": "None", "events": None, "labels": ""}


# ---------------------------------------------------------------------------
# lab state
# ---------------------------------------------------------------------------


@dataclass
class LabState:
    cfg: object
    store: Store
    clock: Clock
    effects: Effects
    hostility: int
    rng: random.Random
    latency_spike_s: float = 5.0
    hang_s: float = 25.0


class RunMismatch(RuntimeError):
    pass


def verify_run(cfg) -> dict:
    """Refuse to serve a world that no longer matches the config it was built from.

    ``hostility``, ``sim_days_per_real_hour`` and ``drop.path`` are free to
    change between restarts.  Anything that determines what the world *is* is
    not: the sealed ledger describes a specific world, and serving a different
    one against it would make every score meaningless.
    """
    from lab.faults import schedule as sched

    ledger = sched.read_ledger(cfgmod.LEDGER_PATH)
    if ledger.get("config_fingerprint") != cfg.fingerprint:
        raise RunMismatch(
            "This world is not the one the sealed ledger was drawn against.\n"
            "\n"
            "A run's identity is [world].scale, [world].horizon_sim_days,\n"
            "[clock].sim_start, [faults].n_sampled, and the lab's own world schema\n"
            "version. One of them has moved.\n"
            "\n"
            "Put the config back, or run `python lab.py init --force` to draw a new\n"
            "run. Forcing discards the current world, its clock, its drop files and\n"
            "everything measured against it."
        )
    return ledger


def build_state(cfg=None, store=None, clock=None, effects=None, hostility=None) -> LabState:
    cfg = cfg or cfgmod.load()
    if effects is None:
        verify_run(cfg)
        effects = Effects.from_ledger()
    return LabState(
        cfg=cfg,
        store=store or Store(),
        clock=clock or Clock(cfg),
        effects=effects,
        hostility=cfg.api.hostility if hostility is None else hostility,
        rng=random.Random(),
    )


# ---------------------------------------------------------------------------
# auth
# ---------------------------------------------------------------------------


class AuthError(Exception):
    def __init__(self, code: str, status: int = 401):
        self.code = code
        self.status = status


def _sign(secret: str, body: str) -> str:
    return hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()[:32]


def issue_token(secret: str, sim_now: dt.datetime, ttl_hours: float) -> tuple[str, dt.datetime]:
    exp = (sim_now + dt.timedelta(hours=ttl_hours)).replace(microsecond=0)
    body = base64.urlsafe_b64encode(
        json.dumps({"exp": exp.isoformat(), "iat": sim_now.isoformat()}).encode()
    ).decode().rstrip("=")
    return f"rlat_{body}.{_sign(secret, body)}", exp


def _bearer(request: Request) -> str:
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        raise AuthError("invalid_token")
    return auth.split(None, 1)[1].strip()


def check_auth(state: LabState, request: Request) -> None:
    token = _bearer(request)
    if token.startswith("rlat_"):
        try:
            body, sig = token[5:].split(".", 1)
            if not hmac.compare_digest(sig, _sign(state.cfg.api.token, body)):
                raise AuthError("invalid_token")
            pad = "=" * (-len(body) % 4)
            claims = json.loads(base64.urlsafe_b64decode(body + pad))
        except AuthError:
            raise
        except Exception:
            raise AuthError("invalid_token")
        if state.clock.now() >= dt.datetime.fromisoformat(claims["exp"]):
            raise AuthError("token_expired")
        return
    if not hmac.compare_digest(token, state.cfg.api.token):
        raise AuthError("invalid_token")
    if state.hostility >= 2:
        # long-lived credentials are no longer honoured on read endpoints
        raise AuthError("invalid_token")


# ---------------------------------------------------------------------------
# cursors
# ---------------------------------------------------------------------------


def encode_cursor(d: dict) -> str:
    raw = json.dumps(d, sort_keys=True, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(s: str) -> dict:
    pad = "=" * (-len(s) % 4)
    return json.loads(base64.urlsafe_b64decode(s + pad))


# ---------------------------------------------------------------------------
# rendering
# ---------------------------------------------------------------------------


def _shift(ts: str | None, delta: dt.timedelta) -> str | None:
    if ts is None or not delta:
        return ts
    try:
        return (dt.datetime.fromisoformat(ts) + delta).isoformat()
    except ValueError:
        return ts


def render_rows(
    table: str, raw_rows, store: Store, as_of: str, policy: RenderPolicy, skew: dt.timedelta
) -> list[dict]:
    out = []
    for row in raw_rows:
        payload = store.serialize(table, row, as_of)
        if table == "orders":
            occurred = dt.datetime.fromisoformat(payload["occurred_at"])
            if policy.extra_order_fields_after and occurred > policy.extra_order_fields_after:
                payload["refund_processed_at"] = row["_refund_ts"]
                payload["support_ticket_count"] = row["_support_tickets"]
            if policy.price_as_major_units_after and occurred > policy.price_as_major_units_after:
                payload["order_total_cents"] = round(payload["order_total_cents"] / 100.0, 2)
            if policy.rename_after and occurred > policy.rename_after:
                if policy.rename_from in payload:
                    payload[policy.rename_to] = payload.pop(policy.rename_from)
            payload["occurred_at"] = _shift(payload["occurred_at"], skew)
            if policy.tz_suffix_after and occurred > policy.tz_suffix_after:
                payload["occurred_at"] = payload["occurred_at"] + policy.tz_suffix
        else:
            for col in ("occurred_at", "valid_from", "valid_to", "resolved_at", "refund_at"):
                if col in payload:
                    payload[col] = _shift(payload[col], skew)
        out.append(check_payload(table, payload))
    return out


def _apply_null_style(obj, style):
    if style is None:
        return obj
    if isinstance(obj, dict):
        return {k: (style if v is None else _apply_null_style(v, style)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_apply_null_style(v, style) for v in obj]
    return obj


# ---------------------------------------------------------------------------
# ambient hostility
# ---------------------------------------------------------------------------


def _err(status: int, code: str, headers: dict | None = None) -> Response:
    return JSONResponse({"error": code}, status_code=status, headers=headers or {})


def pre_request(state: LabState) -> Response | None:
    """Whatever happens to a request before anyone looks at what it asked for."""
    lvl, r = state.hostility, state.rng
    if lvl <= 0:
        return None
    x = r.random()
    if x < 0.020:
        headers = {"Retry-After": "1"}
        if lvl >= 2 and r.random() < 0.45:
            headers = {}
        return _err(429, "rate_limited", headers)
    if x < 0.026:
        return _err(500, "internal_error")
    if lvl >= 2 and r.random() < 0.010:
        time.sleep(state.latency_spike_s * (1.0 + 0.4 * r.random()))
    if lvl >= 3:
        y = r.random()
        if y < 0.010:
            return JSONResponse({"error": "temporary failure"}, status_code=200)
        if y < 0.013:
            time.sleep(state.hang_s)
            return _err(504, "gateway_timeout")
    return None


def finish(state: LabState, endpoint: str, payload: dict) -> Response:
    lvl, r = state.hostility, state.rng
    if lvl >= 2 and "data" in payload:
        # only the records get the house style; the envelope stays honest
        payload = dict(payload)
        payload["data"] = _apply_null_style(payload["data"], _NULL_STYLE.get(endpoint))
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    headers = {"Cache-Control": "no-store"}
    if lvl >= 2:
        want_gzip = r.random() < 0.5
        claim_gzip = want_gzip
        if r.random() < 0.03:
            claim_gzip = not want_gzip
        if want_gzip:
            body = gzip.compress(body)
        if claim_gzip:
            headers["Content-Encoding"] = "gzip"
    return Response(content=body, media_type="application/json", headers=headers)


# ---------------------------------------------------------------------------
# paging
# ---------------------------------------------------------------------------


def _parse_ts(value: str | None, field: str) -> str | None:
    if value is None:
        return None
    try:
        return dt.datetime.fromisoformat(value).replace(microsecond=0).isoformat()
    except ValueError:
        raise ValueError(f"bad {field}")


def collection(state: LabState, endpoint: str, request: Request) -> Response:
    table = ENDPOINT_TABLES[endpoint]
    q = request.query_params
    try:
        limit = int(q.get("limit", DEFAULT_LIMIT))
    except ValueError:
        return _err(400, "bad_limit", None)
    limit = max(1, min(limit, MAX_LIMIT))
    if state.hostility >= 1:
        limit = min(limit, CAPPED_LIMIT)

    cur = q.get("cursor")
    if cur:
        try:
            c = decode_cursor(cur)
        except Exception:
            return _err(400, "bad_cursor", None)
        if c.get("t") != table:
            return _err(400, "bad_cursor", None)
        as_of, since, until = c["a"], c.get("s"), c.get("u")
        pos = (c["v"], c["r"])
        prev = (c.get("pv"), c.get("pr"))
        total = c.get("n")
    else:
        try:
            as_of = _parse_ts(q.get("as_of"), "as_of") or state.clock.now().isoformat()
            since = _parse_ts(q.get("since"), "since")
            until = _parse_ts(q.get("until"), "until")
        except ValueError as exc:
            return _err(400, str(exc), None)
        pos, prev, total = None, (None, None), None

    served_pos, skip = pos, 0
    if state.hostility >= 3 and pos is not None:
        z = state.rng.random()
        if z < 0.0025 and prev[0] is not None:
            served_pos = (prev[0], prev[1])
        elif z < 0.005:
            skip = state.rng.randint(1, 3)

    rows = state.store.page(
        table, as_of, since, until,
        after=tuple(served_pos) if served_pos else None,
        limit=limit + skip,
    )
    if skip:
        if len(rows) > skip:
            anchor = rows[skip - 1]
            served_pos = (anchor[_valid_col(table)], anchor["row_id"])
            rows = rows[skip:]
        else:
            # nothing left to serve past the gap, so the envelope would look
            # honest while rows quietly vanished. Serve the page as asked.
            rows = rows[:limit]

    if total is None:
        total = state.store.count(table, as_of, since, until)

    skew = state.effects.policy.skew(state.clock.now())
    data = render_rows(table, rows, state.store, as_of, state.effects.policy, skew)

    def cursor_for(position, previous):
        d = {"t": table, "a": as_of, "v": position[0], "r": position[1], "n": total}
        if since is not None:
            d["s"] = since
        if until is not None:
            d["u"] = until
        if previous[0] is not None:
            d["pv"], d["pr"] = previous[0], previous[1]
        return encode_cursor(d)

    next_cursor = None
    if len(rows) == limit:
        last = rows[-1]
        next_cursor = cursor_for((last[_valid_col(table)], last["row_id"]), served_pos or (None, None))

    reported_total = total
    if state.hostility >= 3:
        reported_total = max(0, total + state.rng.choice([-2, -1, 0, 1, 2]))

    payload = {
        "data": data,
        "as_of": as_of,
        "cursor": cursor_for(served_pos, prev) if served_pos else None,
        "next_cursor": next_cursor,
        "total_count": reported_total,
    }
    return finish(state, endpoint, payload)


def _valid_col(table: str) -> str:
    from lab.world import SPECS

    return SPECS[table].valid


# ---------------------------------------------------------------------------
# routes
# ---------------------------------------------------------------------------

v1 = APIRouter(prefix="/v1")
lab_router = APIRouter(prefix="/_lab")


@v1.post("/auth/token")
def auth_token(request: Request):
    state: LabState = request.app.state.lab
    try:
        token = _bearer(request)
    except AuthError as exc:
        return _err(exc.status, exc.code)
    if not hmac.compare_digest(token, state.cfg.api.token):
        return _err(401, "invalid_client")
    sim_now = state.clock.now()
    ttl = TOKEN_TTL_SIM_HOURS * (0.75 + 0.5 * state.rng.random())
    access, exp = issue_token(state.cfg.api.token, sim_now, ttl)
    real_seconds = ttl / (state.clock.state.ratio * 24.0) * 3600.0
    return JSONResponse(
        {
            "access_token": access,
            "token_type": "bearer",
            "expires_in": int(max(1, real_seconds)),
            "expires_at_sim": exp.isoformat(),
        }
    )


@v1.get("/health")
def health(request: Request):
    state: LabState = request.app.state.lab
    return JSONResponse(
        {
            "status": "ok",
            "service": "refund-platform-api",
            "version": "2.14.3",
            "server_time": state.clock.now().isoformat(),
        }
    )


def _guard(request: Request) -> tuple[LabState, Response | None]:
    state: LabState = request.app.state.lab
    early = pre_request(state)
    if early is not None:
        return state, early
    try:
        check_auth(state, request)
    except AuthError as exc:
        return state, _err(exc.status, exc.code)
    if state.effects.unavailable(state.clock.now()):
        return state, _err(503, "service_unavailable", {"Retry-After": "120"})
    return state, None


@v1.get("/orders/{order_id}")
def get_order(order_id: str, request: Request):
    state, early = _guard(request)
    if early is not None:
        return early
    try:
        as_of = _parse_ts(request.query_params.get("as_of"), "as_of") or state.clock.now().isoformat()
    except ValueError as exc:
        return _err(400, str(exc))
    row = state.store.one("orders", order_id, as_of)
    if row is None:
        return _err(404, "not_found")
    skew = state.effects.policy.skew(state.clock.now())
    data = render_rows("orders", [row], state.store, as_of, state.effects.policy, skew)[0]
    return finish(state, "orders", {"data": data, "as_of": as_of})


def _collection_route(endpoint: str):
    def handler(request: Request):
        state, early = _guard(request)
        if early is not None:
            return early
        return collection(state, endpoint, request)

    return handler


for _ep in ENDPOINT_TABLES:
    v1.add_api_route(f"/{_ep}", _collection_route(_ep), methods=["GET"], name=f"list_{_ep}")


@lab_router.get("/clock")
def lab_clock(request: Request):
    return JSONResponse(request.app.state.lab.clock.describe())


@lab_router.post("/pause")
def lab_pause(request: Request):
    return JSONResponse(request.app.state.lab.clock.pause())


@lab_router.post("/resume")
def lab_resume(request: Request):
    return JSONResponse(request.app.state.lab.clock.resume())


@lab_router.post("/ratio")
def lab_ratio(request: Request, sim_days_per_real_hour: float):
    try:
        return JSONResponse(request.app.state.lab.clock.set_ratio(sim_days_per_real_hour))
    except ValueError as exc:
        return _err(400, str(exc))


@lab_router.post("/advance")
def lab_advance(request: Request, days: float):
    try:
        return JSONResponse(request.app.state.lab.clock.advance(days))
    except ValueError as exc:
        return _err(400, str(exc))


# ---------------------------------------------------------------------------
# app
# ---------------------------------------------------------------------------


def create_app(state: LabState | None = None, run_drop: bool = True) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if getattr(app.state, "lab", None) is None:
            app.state.lab = build_state()
        task = None
        if run_drop:
            task = await dropmod.start(app.state.lab)
        try:
            yield
        finally:
            if task is not None:
                await dropmod.stop(task)

    app = FastAPI(
        title="Refund Platform API",
        version="2.14.3",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.lab = state
    app.include_router(v1)
    app.include_router(lab_router)
    return app


app = create_app()
