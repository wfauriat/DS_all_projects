"""Static configuration for the pipeline.

Everything the pipeline needs to know about the outside world lives here.  The
vendor's own `lab.toml` is read only for the two facts it is allowed to give us
(the API token and the port); nothing else in the source repository is touched.
"""
from __future__ import annotations

import os
import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parent.parent
SANDBOX = REPO.parent

SOURCE_REPO = SANDBOX / "refund-lab-source"
DROP_DIR = SANDBOX / "drop"

LANDING = REPO / "landing"
LANDING_API = LANDING / "api"
LANDING_DROP = LANDING / "drop"
WAREHOUSE = REPO / "warehouse"
DB_PATH = WAREHOUSE / "wh.db"
LOGS = REPO / "logs"
MODELS = REPO / "models"
STATE = REPO / "state"
INCIDENTS_PATH = REPO / "incidents.jsonl"

for _p in (LANDING_API, LANDING_DROP, WAREHOUSE, LOGS, MODELS, STATE):
    _p.mkdir(parents=True, exist_ok=True)


def _read_lab_toml() -> dict:
    """Pull the API token and port out of the vendor's config file.

    Deliberately a five-line regex rather than a toml parse: we read exactly the
    two keys we are entitled to and ignore the rest of the file.
    """
    txt = (SOURCE_REPO / "lab.toml").read_text(encoding="utf-8")
    out = {}
    m = re.search(r'^\s*token\s*=\s*"([^"]+)"', txt, re.M)
    if m:
        out["token"] = m.group(1)
    m = re.search(r"^\s*port\s*=\s*(\d+)", txt, re.M)
    if m:
        out["port"] = int(m.group(1))
    m = re.search(r'^\s*host\s*=\s*"([^"]+)"', txt, re.M)
    if m:
        out["host"] = m.group(1)
    return out


_lab = _read_lab_toml()
API_HOST = os.environ.get("LAB_HOST", _lab.get("host", "127.0.0.1"))
API_PORT = int(os.environ.get("LAB_PORT", _lab.get("port", 8088)))
API_BASE = f"http://{API_HOST}:{API_PORT}"
API_TOKEN = os.environ.get("LAB_TOKEN", _lab.get("token", ""))

# ---------------------------------------------------------------- extraction
PAGE_LIMIT = 500          # measured server-side cap; asking for more is silently trimmed
MAX_PAGES = 4000          # runaway-pagination guard
HTTP_TIMEOUT = 25.0
MAX_ATTEMPTS = 8

ENTITIES = ("orders", "customers", "events", "labels")

# Business key / valid-time column per API entity.
ENTITY_KEYS = {
    "orders":    (("order_id",), "occurred_at"),
    "customers": (("customer_id", "valid_from"), "valid_from"),
    "events":    (("event_id",), "occurred_at"),
    "labels":    (("order_id",), "resolved_at"),
}

# Fields we expect on each entity.  Anything outside this set is captured in
# `extra_json` and raised as a schema-drift signal rather than dropped.
EXPECTED_FIELDS = {
    "orders": {"order_id", "customer_id", "occurred_at", "channel", "payment_method",
               "shipping_speed", "order_total_cents", "items", "version", "knowledge_time"},
    "customers": {"customer_id", "valid_from", "valid_to", "signup_date", "country", "city",
                  "segment", "lifetime_value_cents", "is_active", "version", "knowledge_time"},
    "events": {"event_id", "customer_id", "session_id", "occurred_at", "event_type", "sku",
               "version", "knowledge_time"},
    "labels": {"order_id", "resolved_at", "refund_at", "refunded_within_30d",
               "version", "knowledge_time"},
}

DROP_FIELDS = {
    "payments": ["payment_id", "order_id", "customer_id", "paid_ts", "amount_cents",
                 "status", "processor", "billing_city"],
    "catalog": ["sku", "product_name", "category", "list_price_cents", "active_from",
                "discontinued_from"],
}

# Documented closed domains.  A value outside these is either a renamed category
# or a genuinely new one -- never baseline behaviour.
DOMAINS = {
    "channel": {"web", "mobile", "partner_api"},
    "payment_method": {"card", "wallet", "bank_transfer", "voucher"},
    "processor": {"northpay", "swiftpay", "orbit"},
    "category": {"apparel", "electronics", "home", "beauty", "outdoor"},
    "segment": {"consumer", "smb", "enterprise"},
    "event_type": {"page_view", "search", "add_to_cart", "remove_from_cart", "checkout_start"},
    "shipping_speed": None,   # not documented; learned from the first 30 days
    "status": None,           # payment status; learned
    "country": None,          # ten countries; learned
}

# ---------------------------------------------------------------- scheduling
STEP_DAYS = int(os.environ.get("STEP_DAYS", "1"))
DROP_WAIT_MAX = 6.0        # seconds to poll for the drop writer before giving up
DROP_WAIT_POLL = 0.1

# Rolling re-pull windows (valid-time days) applied every cycle.
LOOKBACK = {"orders": 3, "customers": 3, "events": 3, "labels": 5}

# Count-reconciliation buckets: (days_back_from, days_back_to, every_n_cycles).
# Each bucket costs one request per entity when it runs; only a bucket whose
# count disagrees gets bisected down to the offending day.  Recent history is
# checked every cycle because that is where detection latency matters; deep
# history rotates, which still finds a change well inside the 60-day window.
RECON_BUCKETS = [(0, 14, 1), (14, 60, 2), (60, 240, 3), (240, 10_000, 5)]

# How many historical days to deep-audit (full re-pull + payload diff) per cycle,
# and how many entities to audit on each of them.
AUDIT_DAYS_PER_CYCLE = 2
AUDIT_ENTITIES_PER_DAY = 2

# Model retraining cadence, in simulated days.
RETRAIN_EVERY_DAYS = 60
LABEL_WINDOW_DAYS = 30      # refund window; labels below this age are censored

# The world starts here; the drop writer's first file is for sim_start + 1 day,
# so nothing before that is a missing delivery.
SIM_START = "2026-01-01"
FIRST_EXPECTED_DROP_DAY = "2026-01-02"

# The customer dimension predates the horizon: most rows have a `valid_from` in
# the years before the world starts, so anything keyed on SIM_START would miss
# almost the whole dimension.
DIM_HISTORY_START = "2018-01-01"


def range_start(entity):
    return DIM_HISTORY_START if entity == "customers" else SIM_START
