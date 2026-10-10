#!/usr/bin/bash
set -euo pipefail
source env.sh
if [[ -f "all_orders.jsonl" ]]; then
    rm all_orders.jsonl
fi
CURSOR="null"
PAGE=1
while true; do
    if [ "$CURSOR" == "null" ]; then
        URL="$BASE/v1/orders?limit=5"
    else
        URL="$BASE/v1/orders?limit=5&cursor=$CURSOR"
    fi
    RESPONSE=$(curl -s -w "\n%{http_code}" "$URL" -H "Authorization: Bearer $TOKEN")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')
    MAX_RETRIES=5
    tries=0
    while (( tries < MAX_RETRIES )); do
        if [ "$HTTP_CODE" == "200" ]; then
            break
        fi
        if [ "$HTTP_CODE" == "401" ]; then
            TOKEN=$(curl -s -X POST "$BASE/v1/auth/token" \
                -H "Authorization: Bearer $LAB_TOKEN" | jq -r .access_token)
        elif [[ "$HTTP_CODE" == "429" || "$HTTP_CODE" == "500" ]]; then
            sleep "$(echo "0.2 * (2^$tries)" | bc)"
        fi
        RESPONSE=$(curl -s -w "\n%{http_code}" "$URL" -H "Authorization: Bearer $TOKEN")
        HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
        BODY=$(echo "$RESPONSE" | sed '$d')
        tries=$((tries + 1))
    done

    if [ "$HTTP_CODE" != "200" ]; then
        echo "ERROR: page $PAGE failed after $MAX_RETRIES retries, last status $HTTP_CODE" >&2
        echo "$BODY" >&2
        exit 1
    fi

    echo "$BODY" | jq -c ".data[]" >> all_orders.jsonl
    CURSOR=$(echo "$BODY" | jq -r '.next_cursor')
    echo "page $PAGE done, next_cursor=$CURSOR"
    PAGE=$((PAGE + 1))
    if [ "$CURSOR" == "null" ]; then
        break
    fi
done