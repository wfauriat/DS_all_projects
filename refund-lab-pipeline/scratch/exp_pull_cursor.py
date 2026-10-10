import httpx, time, json

PORT = "8088"
API_URL = "http://127.0.0.1:" + PORT
DEV_TOKEN = "rl_live_8f2c1d94e6b74a03"

client = httpx.Client(base_url=API_URL, timeout=10,
                      mounts={"all://localhost": None, "all://127.0.0.1": None})
resp = client.post("/v1/auth/token", headers={"Authorization": f"Bearer {DEV_TOKEN}"})
client.headers["Authorization"] = f"Bearer {resp.json()['access_token']}"

def get_with_retry(client, path, params, max_tries=5):
    for attempt in range(max_tries):
        resp = client.get(path, params=params)
        if resp.status_code == 200:
            return resp
        if resp.status_code == 401:
            r = client.post("/v1/auth/token", headers={"Authorization": f"Bearer {DEV_TOKEN}"})
            client.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        else:
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"failed after {max_tries} tries, last status {resp.status_code}")

def full_pull():
    """Paginate all orders, return dict of order_id -> record, plus the as_of seen on page 1."""
    seen = {}
    cursor = None
    as_of_pinned = None
    while True:
        params = {"limit": 20}
        if cursor:
            params["cursor"] = cursor
        r = get_with_retry(client, "/v1/orders", params).json()
        if as_of_pinned is None:
            as_of_pinned = r["as_of"]
        for order in r["data"]:
            seen[order["order_id"]] = order
        cursor = r["next_cursor"]
        if cursor is None:
            break
    return seen, as_of_pinned

print("Pull 1 starting...")
pull1, as_of1 = full_pull()
print(f"Pull 1: {len(pull1)} orders, as_of pinned at {as_of1}")

print("Waiting 90s (sim time keeps moving)...")
# time.sleep(90)
client.post("/_lab/advance", params={"days": 1})

print("Pull 2 starting...")
pull2, as_of2 = full_pull()
print(f"Pull 2: {len(pull2)} orders, as_of pinned at {as_of2}")

only_in_pull2 = set(pull2) - set(pull1)
only_in_pull1 = set(pull1) - set(pull2)
changed = {oid for oid in (set(pull1) & set(pull2)) if pull1[oid] != pull2[oid]}

print(f"New in pull 2: {len(only_in_pull2)}")
print(f"Missing from pull 2 (disappeared?!): {len(only_in_pull1)}")
print(f"Same order_id, different content: {len(changed)}")