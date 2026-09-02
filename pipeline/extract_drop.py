"""The file-drop channel.

There is no manifest, no checksum and no notification, so every fact about a
file has to be established from the bytes:

* compression is decided by the magic number, not the extension;
* encoding is decided by trying UTF-8 strict and falling back, and the fallback
  is recorded;
* truncation is detected from a short gzip stream or a final row with the wrong
  field count;
* the header is compared with the previous file of the same kind, so an added
  or reordered column is noticed at the boundary rather than corrupting a load;
* every file's sha256 is kept, so the same payload arriving twice under two
  names is obvious.

Rows land in ``src_payments`` exactly as delivered -- duplicates included,
because the duplicate is the evidence -- and are deduplicated into
``cur_payments``.
"""
from __future__ import annotations

import csv
import gzip
import io
import re
import shutil
import zlib

from . import config, normalize, util
from .normalize import NormStats

log = util.get_logger("extract_drop")

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
KIND_DIRS = {"payments": ("payments", "payments_"), "catalog": ("catalog", "product_catalog_")}


class DropFile:
    """One delivered file, decoded defensively."""

    def __init__(self, path, kind):
        self.path = path
        self.kind = kind
        self.name = path.name
        self.raw = path.read_bytes()
        self.sha = util.sha256_bytes(self.raw)
        self.n_bytes = len(self.raw)
        self.notes = []
        self.truncated = False
        self.parse_errors = 0

        m = DATE_RE.search(self.name)
        self.file_date = m.group(1) if m else None
        canonical = f"{KIND_DIRS[kind][1]}{self.file_date}"
        if not self.name.startswith(canonical):
            self.notes.append(f"unexpected filename form: {self.name}")

        self.is_gzip = self.raw[:2] == b"\x1f\x8b"
        self.claims_gzip = self.name.endswith(".gz")
        if self.is_gzip != self.claims_gzip:
            self.notes.append("compression does not match extension "
                              f"(magic_gzip={self.is_gzip}, name_gz={self.claims_gzip})")

        data = self.raw
        if self.is_gzip:
            try:
                data = gzip.decompress(self.raw)
            except (EOFError, zlib.error, gzip.BadGzipFile):
                # Salvage whatever decompressed before the stream ran out.
                self.truncated = True
                self.notes.append("gzip stream ends prematurely")
                d = zlib.decompressobj(16 + zlib.MAX_WBITS)
                try:
                    data = d.decompress(self.raw)
                except zlib.error:
                    data = b""

        self.encoding = "utf-8"
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            self.encoding = "cp1252"
            text = data.decode("cp1252", errors="replace")
            self.notes.append("body is not valid UTF-8; decoded as cp1252")
        self.text = text

        if text and not text.endswith("\n"):
            self.truncated = True
            self.notes.append("file does not end with a newline")

        rdr = csv.reader(io.StringIO(text))
        try:
            rows = list(rdr)
        except csv.Error:
            self.parse_errors += 1
            rows = [r for r in csv.reader(io.StringIO(text.replace("\x00", "")))]
        self.header = rows[0] if rows else []
        self.rows = rows[1:] if rows else []
        if self.rows and len(self.rows[-1]) != len(self.header):
            self.truncated = True
            self.notes.append(f"final row has {len(self.rows[-1])} of "
                              f"{len(self.header)} fields")
        self.bad_width = sum(1 for r in self.rows if len(r) != len(self.header))


class DropIngestor:
    def __init__(self, store):
        self.st = store

    # --------------------------------------------------------------- scan
    def scan(self):
        """Every file present on disk that we have not ingested yet."""
        known = {r["file_name"] for r in self.st.q("SELECT file_name FROM drop_files")}
        found = []
        for kind, (sub, _prefix) in KIND_DIRS.items():
            d = config.DROP_DIR / sub
            if not d.is_dir():
                continue
            for p in sorted(d.iterdir()):
                if p.is_file() and p.name not in known:
                    found.append((kind, p))
        return found

    def expect(self, kind, day, due_sim, cycle_id):
        self.st.ex("INSERT OR IGNORE INTO expected_drop(kind,file_date,due_sim,due_cycle) "
                   "VALUES(?,?,?,?)", (kind, day, due_sim, cycle_id))

    # ------------------------------------------------------------- ingest
    def ingest(self, kind, path, sim_now, cycle_id):
        f = DropFile(path, kind)
        prev = self.st.q("SELECT file_name, header_json, sha256, file_date, encoding, compressed "
                         "FROM drop_files WHERE kind=? ORDER BY file_date DESC, file_name DESC "
                         "LIMIT 1", (kind,))
        findings = {"file": f.name, "kind": kind, "file_date": f.file_date,
                    "notes": list(f.notes), "truncated": f.truncated,
                    "header_added": [], "header_removed": [], "header_reordered": False,
                    "encoding_changed": False, "compression_changed": False,
                    "duplicate_of": None, "n_rows": len(f.rows)}

        if prev:
            p = prev[0]
            old_header = list(util.json_loads(p["header_json"])) if p["header_json"] else []
            if old_header and old_header != f.header:
                if set(old_header) == set(f.header):
                    findings["header_reordered"] = True
                else:
                    findings["header_added"] = [c for c in f.header if c not in old_header]
                    findings["header_removed"] = [c for c in old_header if c not in f.header]
            if p["encoding"] != f.encoding:
                findings["encoding_changed"] = True
            if bool(p["compressed"]) != bool(f.is_gzip):
                findings["compression_changed"] = True

        dup = self.st.q("SELECT file_name, file_date FROM drop_files WHERE sha256=? AND kind=?",
                        (f.sha, kind))
        if dup:
            findings["duplicate_of"] = [d["file_name"] for d in dup]

        exp = self.st.q("SELECT due_sim, due_cycle FROM expected_drop WHERE kind=? "
                        "AND file_date=?", (kind, f.file_date))
        due = exp[0]["due_sim"] if exp else None
        due_cycle = exp[0]["due_cycle"] if exp else None
        # A file counts as late only if the pipeline was actually watching for it
        # and did not find it: lateness is measured in cycles we saw it missing,
        # not in wall-clock days.  A backlog that was already on disk when we
        # first looked is our lateness, not the vendor's.
        lag = 0
        if due and due_cycle is not None and cycle_id > due_cycle:
            lag = util.days_between(due, sim_now)
        first_watched = self.st.get_state("watching_drop_from")
        if first_watched and f.file_date and f.file_date < first_watched:
            lag = 0

        # keep the delivered bytes untouched in the landing zone
        dest = config.LANDING_DROP / kind
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dest / f.name)

        self.st.ex("INSERT OR REPLACE INTO drop_files(file_name,kind,file_date,sha256,n_bytes,"
                   "n_rows,encoding,compressed,header_json,truncated,parse_errors,first_seen_sim,"
                   "first_seen_cycle,lag_days,notes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                   (f.name, kind, f.file_date, f.sha, f.n_bytes, len(f.rows), f.encoding,
                    int(f.is_gzip), util.json_dumps(f.header), int(f.truncated), f.parse_errors,
                    sim_now, cycle_id, lag, "; ".join(f.notes)))
        self.st.ex("INSERT INTO expected_drop(kind,file_date,due_sim,arrived_sim,arrived_cycle,"
                   "status) VALUES(?,?,?,?,?,'arrived') ON CONFLICT(kind,file_date) DO UPDATE SET "
                   "arrived_sim=COALESCE(expected_drop.arrived_sim,excluded.arrived_sim),"
                   "arrived_cycle=COALESCE(expected_drop.arrived_cycle,excluded.arrived_cycle),"
                   "status='arrived'", (kind, f.file_date, due or sim_now, sim_now, cycle_id))

        stats = NormStats()
        if kind == "payments":
            findings.update(self._load_payments(f, sim_now, stats))
        else:
            findings.update(self._load_catalog(f, sim_now, stats))
        findings["norm"] = stats
        findings["lag_days"] = lag
        return findings

    # ----------------------------------------------------------- payments
    def _load_payments(self, f: DropFile, sim_now, stats: NormStats):
        cols = f.header
        idx = {c: i for i, c in enumerate(cols)}
        want = config.DROP_FIELDS["payments"]
        # follow a renamed column rather than losing it
        mapping = {}
        for w in want:
            if w in idx:
                mapping[w] = idx[w]
        missing = [w for w in want if w not in mapping]
        extras = [c for c in cols if c not in want]
        if missing and extras:
            import difflib
            pool = list(extras)
            for w in missing:
                best = max(pool, key=lambda c: difflib.SequenceMatcher(None, w, c).ratio(),
                           default=None)
                if best and difflib.SequenceMatcher(None, w, best).ratio() >= 0.55:
                    mapping[w] = idx[best]
                    pool.remove(best)

        rows, seen_hash, dup_exact = [], {}, 0
        for i, r in enumerate(f.rows):
            if len(r) < len(cols):
                r = r + [None] * (len(cols) - len(r))

            def g(name, r=r):
                j = mapping.get(name)
                return r[j] if j is not None and j < len(r) else None
            extra = {c: r[idx[c]] for c in extras if idx[c] < len(r)} or None
            rec = {c: (r[idx[c]] if idx[c] < len(r) else None) for c in cols}
            h = util.row_hash(rec)
            if h in seen_hash:
                dup_exact += 1
            seen_hash[h] = seen_hash.get(h, 0) + 1
            rows.append((
                f.name, i, f.file_date,
                normalize.norm_id(g("payment_id"), "payment_id", stats),
                normalize.norm_id(g("order_id"), "order_id", stats),
                normalize.norm_id(g("customer_id"), "customer_id", stats),
                normalize.norm_ts(g("paid_ts"), "paid_ts", stats),
                normalize.norm_num(g("amount_cents"), "amount_cents", stats),
                normalize.norm_text(g("status"), "status", stats),
                normalize.norm_text(g("processor"), "processor", stats),
                normalize.norm_text(g("billing_city"), "billing_city", stats),
                util.json_dumps(extra) if extra else None, h, sim_now))
            stats.rows += 1
        self.st.exm("INSERT OR IGNORE INTO src_payments(file_name,row_no,file_date,payment_id,"
                    "order_id,customer_id,paid_ts,amount_cents,status,processor,billing_city,"
                    "extra_json,row_hash,first_seen_sim) VALUES(" + ",".join("?" * 14) + ")", rows)

        # deduplicate into current belief
        cur = []
        for t in rows:
            pid, paid = t[3], t[6]
            lag = None
            a = util.parse_ts(paid)[0]
            b = util.to_day(f.file_date)
            if a:
                lag = int((b - a.date()).days)
            cur.append((pid, t[4], t[5], paid, (paid or "")[:10], t[7], t[8], t[9], t[10],
                        f.file_date, f.file_date, lag, sim_now))
        # A payment re-delivered later with a different amount is a restatement,
        # not a new payment and not a reused id: current belief takes the newer
        # delivery, and src_payments keeps both.
        self.st.exm(
            "INSERT INTO cur_payments(payment_id,order_id,customer_id,paid_ts,paid_day,"
            "amount_cents,status,processor,billing_city,first_file_date,last_file_date,"
            "settle_lag_seconds,first_seen_sim) VALUES(" + ",".join("?" * 13) + ") "
            "ON CONFLICT(payment_id) DO UPDATE SET n_deliveries=cur_payments.n_deliveries+1,"
            "n_revisions=cur_payments.n_revisions+"
            "(cur_payments.amount_cents IS NOT excluded.amount_cents),"
            "order_id=excluded.order_id,customer_id=excluded.customer_id,"
            "paid_ts=excluded.paid_ts,paid_day=excluded.paid_day,"
            "amount_cents=excluded.amount_cents,status=excluded.status,"
            "processor=excluded.processor,billing_city=excluded.billing_city,"
            "last_file_date=excluded.last_file_date,"
            "settle_lag_seconds=excluded.settle_lag_seconds "
            "WHERE excluded.last_file_date >= "
            "COALESCE(cur_payments.last_file_date, cur_payments.first_file_date)",
            cur)
        revised = [t[3] for t in rows]
        return {"dup_exact_rows": dup_exact, "extras": extras, "missing": missing,
                "payment_ids": revised,
                "mapped": {k: cols[v] for k, v in mapping.items() if cols[v] != k}}

    # ------------------------------------------------------------ catalog
    def _load_catalog(self, f: DropFile, sim_now, stats: NormStats):
        cols = f.header
        idx = {c: i for i, c in enumerate(cols)}
        want = config.DROP_FIELDS["catalog"]
        mapping = {w: idx[w] for w in want if w in idx}
        missing = [w for w in want if w not in mapping]
        extras = [c for c in cols if c not in want]
        if missing and extras:
            import difflib
            pool = list(extras)
            for w in missing:
                best = max(pool, key=lambda c: difflib.SequenceMatcher(None, w, c).ratio(),
                           default=None)
                if best and difflib.SequenceMatcher(None, w, best).ratio() >= 0.55:
                    mapping[w] = idx[best]
                    pool.remove(best)

        src_rows, cur_rows, skus = [], [], []
        for r in f.rows:
            def g(name, r=r):
                j = mapping.get(name)
                return r[j] if j is not None and j < len(r) else None
            extra = {c: r[idx[c]] for c in extras if idx[c] < len(r)} or None
            sku = normalize.norm_id(g("sku"), "sku", stats)
            if not sku:
                continue
            name = normalize.norm_text(g("product_name"), "product_name", stats)
            cat = normalize.norm_text(g("category"), "category", stats)
            price = normalize.norm_num(g("list_price_cents"), "list_price_cents", stats)
            af = normalize.norm_text(g("active_from"), "active_from", stats)
            df = normalize.norm_text(g("discontinued_from"), "discontinued_from", stats)
            ej = util.json_dumps(extra) if extra else None
            h = util.row_hash([name, cat, price, af, df, ej])
            src_rows.append((sku, h, name, cat, price, af, df, ej, f.file_date, f.file_date))
            cur_rows.append((sku, name, cat, price, af, df, f.file_date, f.file_date))
            skus.append(sku)
            stats.rows += 1
        self.st.exm("INSERT INTO src_products(sku,row_hash,product_name,category,"
                    "list_price_cents,active_from,discontinued_from,extra_json,first_file_date,"
                    "last_file_date) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(sku,row_hash) "
                    "DO UPDATE SET last_file_date=excluded.last_file_date", src_rows)
        self.st.exm(
            "INSERT INTO cur_products(sku,product_name,category,list_price_cents,active_from,"
            "discontinued_from,last_file_date,first_file_date) VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(sku) DO UPDATE SET "
            "n_changes=cur_products.n_changes+(cur_products.product_name IS NOT excluded.product_name"
            " OR cur_products.category IS NOT excluded.category"
            " OR cur_products.list_price_cents IS NOT excluded.list_price_cents),"
            "product_name=excluded.product_name,category=excluded.category,"
            "list_price_cents=excluded.list_price_cents,active_from=excluded.active_from,"
            "discontinued_from=excluded.discontinued_from,last_file_date=excluded.last_file_date,"
            "missing_since=NULL", cur_rows)
        return {"extras": extras, "missing": missing, "n_skus": len(skus),
                "mapped": {k: cols[v] for k, v in mapping.items() if cols[v] != k}}
