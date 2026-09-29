"""Generate a small marketplace dataset (snowflake schema) as CSV + log files.

Run:  python gen_marketplace.py
Out:  ./data/*.csv, ./data/web_events.log, ./data/payments.jsonl

Schema (snowflake: some dimensions are normalized into sub-dimensions)

  dim_country <- dim_city <- dim_customer
                        \\<- dim_seller <- dim_product -> dim_category (self-ref parent)
  fact_orders <- fact_order_items -> dim_product
  payments.jsonl   -> semi-structured, one JSON per order (nested fields)
  web_events.log   -> raw text log, needs parsing (contains a few junk lines)
"""
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

SEED = 42
N_CUSTOMERS, N_SELLERS, N_PRODUCTS, N_ORDERS = 200, 25, 120, 1500
START, END = datetime(2025, 1, 1), datetime(2025, 6, 30)

random.seed(SEED)
out = Path("data")
out.mkdir(exist_ok=True)


def write_csv(name, header, rows):
    with open(out / f"{name}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"{name}.csv: {len(rows)} rows")


def rand_ts(lo=START, hi=END):
    return lo + timedelta(seconds=random.randint(0, int((hi - lo).total_seconds())))


# ---------- dimensions ----------
countries = [(1, "France", "EU"), (2, "Germany", "EU"), (3, "Spain", "EU"),
             (4, "USA", "NA"), (5, "Canada", "NA")]
write_csv("dim_country", ["country_id", "country_name", "region"], countries)

cities = [(1, "Paris", 1), (2, "Lyon", 1), (3, "Berlin", 2), (4, "Munich", 2),
          (5, "Madrid", 3), (6, "Barcelona", 3), (7, "New York", 4),
          (8, "Austin", 4), (9, "Toronto", 5), (10, "Montreal", 5)]
write_csv("dim_city", ["city_id", "city_name", "country_id"], cities)

# parent_category_id is empty for top-level categories (NULL on ingestion)
categories = [(1, "Electronics", None), (2, "Home", None), (3, "Fashion", None),
              (4, "Phones", 1), (5, "Laptops", 1), (6, "Audio", 1),
              (7, "Kitchen", 2), (8, "Furniture", 2),
              (9, "Shoes", 3), (10, "Jackets", 3)]
write_csv("dim_category", ["category_id", "category_name", "parent_category_id"],
          [(i, n, "" if p is None else p) for i, n, p in categories])

first = ["Ana", "Ben", "Chloe", "Dan", "Eva", "Farid", "Gina", "Hugo", "Iris",
         "Jon", "Kim", "Leo", "Mia", "Noah", "Olga", "Paul", "Quinn", "Rosa"]
last = ["Martin", "Bernard", "Dubois", "Muller", "Garcia", "Smith", "Tremblay",
        "Lopez", "Weber", "Brown", "Roy", "Petit"]

customers = []
for cid in range(1, N_CUSTOMERS + 1):
    fn, ln = random.choice(first), random.choice(last)
    email = f"{fn}.{ln}{cid}@example.com".lower() if random.random() > 0.05 else ""
    customers.append((cid, f"{fn} {ln}", email, random.choice(cities)[0],
                      rand_ts(datetime(2024, 1, 1), END).date().isoformat()))
write_csv("dim_customer", ["customer_id", "name", "email", "city_id", "signup_date"],
          customers)

sellers = []
for sid in range(1, N_SELLERS + 1):
    rating = round(random.uniform(2.5, 5), 1) if random.random() > 0.15 else ""
    sellers.append((sid, f"Shop {sid:02d}", random.choice(cities)[0],
                    rand_ts(datetime(2023, 1, 1), datetime(2024, 12, 31)).date().isoformat(),
                    rating))
write_csv("dim_seller", ["seller_id", "seller_name", "city_id", "joined_date", "rating"],
          sellers)

leaf_cats = [c[0] for c in categories if c[2] is not None]
adjectives = ["Pro", "Mini", "Max", "Eco", "Classic", "Ultra", "Lite"]
products = []
for pid in range(1, N_PRODUCTS + 1):
    cat = random.choice(leaf_cats)
    cat_name = next(c[1] for c in categories if c[0] == cat)
    products.append((pid, f"{cat_name} {random.choice(adjectives)} {pid}", cat,
                     random.randint(1, N_SELLERS),
                     round(random.uniform(5, 900), 2)))
write_csv("dim_product", ["product_id", "product_name", "category_id", "seller_id", "list_price"],
          products)
price_of = {p[0]: p[4] for p in products}

# ---------- facts ----------
orders, items, payments = [], [], []
item_id = 1
for oid in range(1, N_ORDERS + 1):
    cust = random.randint(1, N_CUSTOMERS)
    ts = rand_ts()
    status = random.choices(["completed", "shipped", "cancelled", "refunded"],
                            weights=[70, 15, 10, 5])[0]
    orders.append((oid, cust, ts.strftime("%Y-%m-%d %H:%M:%S"), status))

    total = 0.0
    for pid in random.sample(range(1, N_PRODUCTS + 1), random.choices([1, 2, 3, 4], [50, 30, 15, 5])[0]):
        qty = random.choices([1, 2, 3], [75, 20, 5])[0]
        unit = round(price_of[pid] * random.uniform(0.9, 1.1), 2)  # price drift
        disc = random.choice([0, 0, 0, 5, 10, 20]) if random.random() > 0.1 else ""  # 10% missing
        items.append((item_id, oid, pid, qty, unit, disc))
        total += qty * unit * (1 - (disc or 0) / 100)
        item_id += 1

    payments.append({
        "order_id": oid,
        "paid_at": (ts + timedelta(minutes=random.randint(1, 30))).isoformat() + "Z",
        "method": random.choice(["card", "paypal", "transfer"]),
        "amount": round(total, 2),
        "card": {"brand": random.choice(["visa", "mc", "amex"]),
                 "last4": f"{random.randint(0, 9999):04d}"} if random.random() > 0.4 else None,
    })

write_csv("fact_orders", ["order_id", "customer_id", "order_ts", "status"], orders)
write_csv("fact_order_items",
          ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct"],
          items)

with open(out / "payments.jsonl", "w") as f:
    for p in payments:
        f.write(json.dumps(p) + "\n")
print(f"payments.jsonl: {len(payments)} rows")

# ---------- raw log ----------
events = []
for _ in range(6000):
    events.append((rand_ts(), random.choice(["view", "view", "view", "add_to_cart", "search"]),
                   random.randint(1, N_CUSTOMERS), random.randint(1, N_PRODUCTS)))
lines = []
for ts, ev, cid, pid in sorted(events):
    sess = f"s{random.randint(1000, 9999)}"
    lines.append(f"{ts.isoformat()}Z INFO user={cid} event={ev} product={pid} session={sess}")
for _ in range(40):  # junk lines to make parsing interesting
    i = random.randint(0, len(lines) - 1)
    lines.insert(i, random.choice(["", "-- restart --", "ERROR upstream timeout",
                                   "2025-03-01T00:00:00Z WARN user=? event=view"]))
(out / "web_events.log").write_text("\n".join(lines) + "\n")
print(f"web_events.log: {len(lines)} lines")