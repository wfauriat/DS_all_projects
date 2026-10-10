export no_proxy="localhost,127.0.0.1"

PORT=8088
BASE=http://127.0.0.1:$PORT

LAB_TOKEN=$(grep '^token' lab.toml | cut -d '"' -f2)
TOKEN=$(curl -s -X POST "$BASE/v1/auth/token" -H "Authorization: Bearer $LAB_TOKEN" | jq -r .access_token)
