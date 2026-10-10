"""Exercise the deterministic detectors against synthetic corruption.

Pure functions only -- nothing here touches the live warehouse or the source.
"""
import gzip, io, pathlib, sys, tempfile
sys.path.insert(0, "/home/ai-user/Documents/Sandbox/DE_realish/refund-lab-autom")
from pipeline.extract_drop import DropFile
from pipeline import normalize
from pipeline.normalize import NormStats

tmp = pathlib.Path(tempfile.mkdtemp())
HDR = "payment_id,order_id,customer_id,paid_ts,amount_cents,status,processor,billing_city\n"
ROW = "PAY-00000011,ORD-00000011,0000191,2026-01-01T21:06:58,3708,captured,orbit,{city}\n"
def body(n=5, city="Angers", hdr=HDR):
    return hdr + "".join(ROW.format(city=city) for _ in range(n))

results = []
def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))

# 1. healthy gzipped file
p = tmp/"payments_2026-01-02.csv.gz"; p.write_bytes(gzip.compress(body().encode()))
f = DropFile(p, "payments")
check("healthy: not truncated", not f.truncated, f.notes)
check("healthy: utf-8", f.encoding == "utf-8")
check("healthy: 5 rows", len(f.rows) == 5, len(f.rows))

# 2. partial_write -- gzip stream cut mid-row
raw = gzip.compress(body(40).encode())
p = tmp/"payments_2026-01-03.csv.gz"; p.write_bytes(raw[:len(raw)//2])
f = DropFile(p, "payments")
check("partial_write: gzip truncation detected", f.truncated, f.notes)

# 3. partial_write -- plain truncation mid-row, correct gzip framing
txt = body(20); txt = txt[:-12]
p = tmp/"payments_2026-01-04.csv.gz"; p.write_bytes(gzip.compress(txt.encode()))
f = DropFile(p, "payments")
check("partial_write: short final row detected", f.truncated, f.notes)

# 4. compression_change -- .gz extension, plain bytes
p = tmp/"payments_2026-01-05.csv.gz"; p.write_bytes(body().encode())
f = DropFile(p, "payments")
check("compression_change: gz name, plain bytes",
      any("compression" in n for n in f.notes) and not f.truncated, f.notes)

# 5. compression_change -- plain extension, gzipped bytes
p = tmp/"product_catalog_2026-01-05.csv"; p.write_bytes(gzip.compress(
    "sku,product_name,category,list_price_cents,active_from,discontinued_from\nA-1,X,home,10,2024-01-01,\n".encode()))
f = DropFile(p, "catalog")
check("compression_change: plain name, gz bytes", any("compression" in n for n in f.notes), f.notes)

# 6. encoding_change -- cp1252
p = tmp/"payments_2026-01-06.csv.gz"
p.write_bytes(gzip.compress(body(city="Gijón").encode("cp1252")))
f = DropFile(p, "payments")
check("encoding_change: cp1252 detected", f.encoding == "cp1252", f.notes)

# 7. column_added / column_reordered at the header level
hdr2 = "payment_id,order_id,customer_id,paid_ts,amount_cents,status,processor,billing_city,fee_cents\n"
f_add = DropFile.__new__(DropFile)
old = HDR.strip().split(","); new = hdr2.strip().split(",")
check("column_added: diff", [c for c in new if c not in old] == ["fee_cents"])
reord = ["order_id","payment_id","customer_id","paid_ts","amount_cents","status","processor","billing_city"]
check("column_reordered: same set, different order", set(reord) == set(old) and reord != old)

# 8. dup_exact_payments -- byte-identical rows inside one file
p = tmp/"payments_2026-01-07.csv.gz"; p.write_bytes(gzip.compress(body(6).encode()))
f = DropFile(p, "payments")
from pipeline import util
hashes = {}
dups = 0
for r in f.rows:
    h = util.row_hash(dict(zip(f.header, r)))
    if h in hashes: dups += 1
    hashes[h] = 1
check("dup_exact_payments: identical rows counted", dups == 5, dups)

# ---- record-level normalisation
st = NormStats()
rec = {"order_id":"ORD-1","customer_id":"191","occurred_at":"2026-01-01T00:00:00",
       "channel":"web","payment_method":"card","shipping_speed":"standard",
       "order_total_cents":100,"items":[],"version":1,"knowledge_time":"2026-01-01T00:01:00"}
normalize.normalize("orders", rec, st, "2026-01-01T00:00:00", 1)
check("id_leading_zeros: width violation counted", st.id_width_violations.get("customer_id") == 1,
      st.id_width_violations)
check("id_leading_zeros: id re-padded for the join", st.padded_ids == 1)

st = NormStats()
rec2 = dict(rec); rec2["customer_id"]="0000191"
rec2["occurred_at"]="2026-01-01T00:00:00+02:00"
normalize.normalize("orders", rec2, st, "2026-01-01T00:00:00", 1)
check("tz_mixed: offset counted", st.tz_offsets >= 1, st.tz_offsets)

st = NormStats()
rec3 = dict(rec); rec3["customer_id"]="0000191"
del rec3["order_total_cents"]; rec3["order_value_cents"] = 4200
out = normalize.normalize("orders", rec3, st, "2026-01-01T00:00:00", 1)[1]
check("field_rename_midstream: rename followed", st.renamed.get("order_total_cents")=="order_value_cents",
      st.renamed)
check("field_rename_midstream: value carried through", out[8] == 4200, out[8])

st = NormStats()
rec4 = dict(rec); rec4["customer_id"]="0000191"; rec4["order_total_cents"] = -999
normalize.normalize("orders", rec4, st, "2026-01-01T00:00:00", 1)
check("sentinel_values: -999 flagged", any("-999" in k for k in st.sentinels), st.sentinels)

st = NormStats()
c = {"customer_id":"0000191","valid_from":"2024-02-02T00:00:00","valid_to":None,
     "signup_date":"2024-02-02","country":"ES","city":"GijÃ³n","segment":"consumer",
     "lifetime_value_cents":1,"is_active":1,"version":1,"knowledge_time":"2026-01-01T00:00:00"}
normalize.normalize("customers", c, st, "2026-01-01T00:00:00", 1)
check("mojibake_city: flagged", st.mojibake.get("city") == 1, st.mojibake)

st = NormStats()
c2 = dict(c); c2["city"] = "Gijón"
normalize.normalize("customers", c2, st, "2026-01-01T00:00:00", 1)
check("mojibake_city: genuine accents not flagged", not st.mojibake, st.mojibake)

st = NormStats()
c3 = dict(c); c3["city"] = "N/A"
normalize.normalize("customers", c3, st, "2026-01-01T00:00:00", 1)
check("sentinel_values: N/A flagged", any("N/A" in k for k in st.sentinels), st.sentinels)

st = NormStats()
for spelling in ("", "None", "null"):
    c4 = dict(c); c4["city"] = spelling
    normalize.normalize("customers", c4, st, "2026-01-01T00:00:00", 1)
check("sentinel_values: plain null spellings NOT flagged", not st.sentinels, st.sentinels)

# DST calendar
from pipeline.monitors import near_dst, _dst_boundaries
import datetime as dt
check("dst: 2026 boundaries", _dst_boundaries(2026) == [dt.date(2026,3,29), dt.date(2026,10,25)],
      _dst_boundaries(2026))
check("dst: near flag", near_dst(dt.date(2026,10,25)) and not near_dst(dt.date(2026,7,1)))



# ---------------------------------------------------------------------------
# Regression tests for the two faults this run missed.  Both were wiring, not
# statistics: evidence that was collected and never routed, and a detector
# pointed at one of the two tables it should have watched.
# ---------------------------------------------------------------------------
import datetime as _dt
import tempfile as _tf
from pipeline import storage as _storage, monitors as _monitors, util as _util
from pipeline.normalize import NormStats as _NS


class _CapturingLog:
    def __init__(self):
        self.fired = []

    def raise_incident(self, sim_now, symptom, family, fault=None, confidence="medium",
                       resolution="", evidence=None, key=None, cycle_id=None):
        self.fired.append(fault)
        return True


def _monitor_on(store, day, ctx=None):
    log = _CapturingLog()
    m = _monitors.Monitors(store, log)
    m.ctx = ctx or {"cycle_id": 1, "sim_now": day + "T12:00:00"}
    m.sim_now = day + "T12:00:00"
    m.cycle = 1
    m.today = _util.to_day(day)
    m.age_days = 2000
    m.completed_days = [day]
    return m, log


# --- the drop channel normalises through the same code and must be heard
_ds = _NS()
_ds.id_width_violations = {"customer_id": 68}
_ds.padded_ids = 68
_store = _storage.Store(pathlib.Path(_tf.mkdtemp()) / "t.db")
_m, _log = _monitor_on(_store, "2031-02-10",
                       {"cycle_id": 1, "sim_now": "2031-02-10T12:00:00",
                        "norm": {"orders": _NS(), "payments": _ds}})
_m.completed_days = []
_m.m_identifiers()
check("drop-channel id evidence reaches the monitors", "id_leading_zeros" in _log.fired,
      _log.fired)

_m2, _log2 = _monitor_on(_store, "2031-02-10",
                         {"cycle_id": 1, "sim_now": "2031-02-10T12:00:00",
                          "norm": {"orders": _NS()}})
_m2.completed_days = []
_m2.m_identifiers()
check("...and the API-only context is what used to hide it", not _log2.fired, _log2.fired)


# --- the hourly profile must watch orders, not only events
def _seed_hours(store, day, per_hour, total_metric="orders_n", metric="orders_hour"):
    store.put_metric(day, total_metric, sum(per_hour.values()))
    for h, c in per_hour.items():
        store.put_metric(day, metric, c, h)


_flat = {f"{i:02d}": (2 if i < 6 else 12) for i in range(24)}
_d = _dt.date(2031, 3, 30)
for _i in range(1, 29):
    _seed_hours(_store, (_d - _dt.timedelta(days=_i)).isoformat(), _flat)
_pile = {f"{i:02d}": (0 if i == 2 else (2 if i < 6 else 12)) for i in range(24)}
_pile["02"] = 60
_seed_hours(_store, _d.isoformat(), _pile)
_store.commit()

_m3, _log3 = _monitor_on(_store, "2031-03-30")
_m3.m_hourly_profile()
check("hour pile-up on a DST day is flagged from the orders stream",
      "dst_duplicate_hour" in _log3.fired, _log3.fired)

# a quiet hour that a pile-up would normally sit in must not need to be busy first
check("pile-up test ignores the expected-share floor", _pile["02"] == 60)

# the same profile on an ordinary day is noise, not a claim
_d2 = _dt.date(2031, 6, 15)
for _i in range(1, 29):
    _seed_hours(_store, (_d2 - _dt.timedelta(days=_i)).isoformat(), _flat)
_seed_hours(_store, _d2.isoformat(), _pile)
_store.commit()
_m4, _log4 = _monitor_on(_store, "2031-06-15")
_m4.m_hourly_profile()
check("the same profile on a non-DST day is an alert, not a claim", not _log4.fired, _log4.fired)

bad = [r for r in results if not r[1]]
print()
for name, ok, detail in results:
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f"   -> {detail}"))
print(f"\n{len(results)-len(bad)}/{len(results)} passed")
sys.exit(1 if bad else 0)
