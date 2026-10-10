import httpx
import pprint as pp
import base64

PORT = "8088"
API_URL = "http://127.0.0.1:" + PORT
DEV_TOKEN = "rl_live_8f2c1d94e6b74a03"


def auth_to_API(client: httpx.Client):
    resp = client.post(API_URL + "/v1/auth/token",
                   headers={"Authorization": f"Bearer {DEV_TOKEN}"})
    live_token = resp.json()["access_token"]
    client.headers["Authorization"] = f"Bearer {live_token}"

# response = httpx.post(API_URL + "/v1/auth/token",
#                    headers={"Authorization": f"Bearer {DEV_TOKEN}"})
# token = response.json()["access_token"]

client = httpx.Client(base_url=API_URL, timeout=10,
                      mounts={"all://localhost": None,
                              "all://127.0.0.1": None})
auth_to_API(client)

response = client.get(API_URL + "/v1/orders",
                     params={"limit":50,
                             "as_of":"2026-06-01T00:00:00",
                             "since":"2026-01-01T00:00:00",
                             "until":"2026-06-01T00:00:00"})

pp.pprint(response.json()["data"][0])


def b64decode_relaxed(s):
    s = s.strip()
    s += "=" * (-len(s) % 4)
    return base64.b64decode(s)

print(b64decode_relaxed(response.json()["next_cursor"]))