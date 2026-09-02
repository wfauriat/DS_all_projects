#!/usr/bin/env bash
#
# Hand-testing probes for the vendor API.
#
#   ./explore.sh scratch [dir]   build a throwaway lab to poke at (default /tmp/labplay)
#   ./explore.sh probe           run the probes against a running instance
#   ./explore.sh probe -a 300    ...after jumping the clock 300 simulated days
#
# Every probe is a single request you could have typed. That is the point: this
# is a curl cookbook with the arguments already filled in, not a client.
#
# It deliberately does NOT retry, back off, re-page, validate the echoed cursor
# or deduplicate. At hostility 1+ some probes below will simply fail, and the
# script reports the status code and moves on. Turning that into an extraction
# that survives is the exercise, and it belongs in the pipeline repo.
#
# Config is read from lab.toml (or $REFUND_LAB_CONFIG). Override with the
# environment: API=host:port TOKEN=... ./explore.sh probe

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF="${REFUND_LAB_CONFIG:-$REPO/lab.toml}"
PY="${PYTHON:-python3}"

die() { echo "explore: $*" >&2; exit 1; }
hdr() { printf '\n== %s\n' "$*"; }
note() { printf '   %s\n' "$*"; }

for tool in curl jq "$PY"; do
    command -v "$tool" >/dev/null 2>&1 || die "need $tool on PATH"
done
[ -f "$CONF" ] || die "no config at $CONF"

# crude but sufficient TOML scalar reader: key = value, first match wins
tomlv() {
    grep -E "^[[:space:]]*$1[[:space:]]*=" "$CONF" | head -1 \
        | sed -e 's/#.*//' -e 's/^[^=]*=//' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' \
        | tr -d '"'
}

# ---------------------------------------------------------------------------
# scratch: a separate world to experiment on
# ---------------------------------------------------------------------------

cmd_scratch() {
    local dir="${1:-/tmp/labplay}"
    mkdir -p "$dir" || die "cannot create $dir"
    sed -e 's#^path = .*#path = "'"$dir"'/drop"#' \
        -e 's/^hostility = .*/hostility = 0/' \
        -e 's/^scale = .*/scale = 0.1/' \
        -e 's/^horizon_sim_days = .*/horizon_sim_days = 900/' \
        -e 's/^sim_days_per_real_hour = .*/sim_days_per_real_hour = 24.0/' \
        -e 's/^port = .*/port = 8090/' \
        "$REPO/lab.toml" > "$dir/lab.toml" || die "cannot write $dir/lab.toml"

    REFUND_LAB_CONFIG="$dir/lab.toml" REFUND_LAB_STATE="$dir/state" \
        "$PY" "$REPO/lab.py" init --force || die "init failed"

    cat <<EOF

A throwaway world, unrelated to your sealed run: different seed, different
faults, small, fast (24 simulated days per real hour), hostility 0, port 8090.
Use 'probe -a N' to jump further; the drop writer then catches up at roughly 30
simulated days per second, so give it a moment before looking at the files.

Serve it:

    REFUND_LAB_CONFIG=$dir/lab.toml REFUND_LAB_STATE=$dir/state python lab.py serve

Then, in another terminal:

    REFUND_LAB_CONFIG=$dir/lab.toml ./explore.sh probe -a 300

Nothing here touches .state/ or the drop directory of your real run.
EOF
}

# ---------------------------------------------------------------------------
# probe
# ---------------------------------------------------------------------------

api_get() { curl -s --compressed -H "$AUTHHDR" "$API$1"; }
api_code() { curl -s -o /dev/null -w '%{http_code}' --compressed -H "$AUTHHDR" "$API$1"; }

# show a body if it parsed, otherwise say what actually came back
show() {
    local path="$1" filter="${2:-.}" body code
    body=$(api_get "$path")
    if jq -e . >/dev/null 2>&1 <<<"$body"; then
        jq -c "$filter" <<<"$body"
    else
        code=$(api_code "$path")
        case "$code" in
            401) note "HTTP 401 -- the access token expired mid-probe. Re-run; handling this" \
                      "mid-pull is yours to solve." ;;
            *)   note "HTTP $code, body not JSON (${#body} bytes) -- expected above hostility 0" ;;
        esac
    fi
}

shift_sim() {  # $1 = fraction between sim_start and sim_now
    "$PY" - "$SIM_START" "$SIM_NOW" "$1" <<'EOF'
import datetime as dt, sys
a = dt.datetime.fromisoformat(sys.argv[1]); b = dt.datetime.fromisoformat(sys.argv[2])
print((a + (b - a) * float(sys.argv[3])).replace(microsecond=0).isoformat())
EOF
}

plus_days() { "$PY" -c "import datetime as dt,sys;print((dt.datetime.fromisoformat(sys.argv[1])+dt.timedelta(days=float(sys.argv[2]))).isoformat())" "$1" "$2"; }

cmd_probe() {
    local advance=0
    while [ $# -gt 0 ]; do
        case "$1" in
            -a|--advance) advance="${2:-0}"; shift 2 ;;
            *) die "unknown option $1" ;;
        esac
    done

    local host port
    host=$(tomlv host); port=$(tomlv port)
    API="${API:-${host:-127.0.0.1}:${port:-8088}}"
    TOKEN="${TOKEN:-$(tomlv token)}"
    [ -n "$TOKEN" ] || die "no token in $CONF and none in \$TOKEN"

    curl -s -o /dev/null --max-time 5 "$API/v1/health" \
        || die "nothing answering on $API -- is the service running?"

    hdr "health and clock (no auth needed)"
    curl -s "$API/v1/health" | jq -c .
    curl -s "$API/_lab/clock" | jq -c '{sim_now, sim_end, elapsed_sim_days, paused, at_horizon_end}'

    if [ "${advance%.*}" != "0" ]; then
        hdr "advancing the clock $advance simulated days"
        note "this is recorded in the run and reported by the grader"
        curl -s -X POST "$API/_lab/advance?days=$advance" | jq -c '{sim_now, elapsed_sim_days}'
    fi

    SIM_START=$(curl -s "$API/_lab/clock" | jq -r .sim_start)
    SIM_NOW=$(curl -s "$API/_lab/clock" | jq -r .sim_now)

    hdr "authentication"
    local code
    code=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $TOKEN" "$API/v1/orders?limit=1")
    if [ "$code" = "401" ]; then
        local access
        access=$(curl -s -X POST "$API/v1/auth/token" -H "Authorization: Bearer $TOKEN" | jq -r '.access_token // empty')
        [ -n "$access" ] || die "could not obtain an access token"
        AUTHHDR="Authorization: Bearer $access"
        note "long-lived token refused (HTTP 401); exchanged it at /v1/auth/token"
        curl -s -X POST "$API/v1/auth/token" -H "Authorization: Bearer $TOKEN" \
            | jq -c '{token_type, expires_in, expires_at_sim}'
        note "that expiry is real seconds -- a long pull will need another one"
    else
        AUTHHDR="Authorization: Bearer $TOKEN"
        note "long-lived token accepted directly (HTTP $code)"
    fi

    hdr "one order, whole record"
    show "/v1/orders?limit=1" '.data[0]'

    hdr "as_of: the same question at three moments of knowledge"
    local f
    for f in 0.25 0.5 1.0; do
        local ao; ao=$(shift_sim "$f")
        printf '   as_of=%s  orders=%s\n' "$ao" \
            "$(api_get "/v1/orders?as_of=$ao&limit=1" | jq -r '.total_count // "?"')"
    done

    hdr "since/until filter valid time, not knowledge time"
    local lo hi; lo=$(shift_sim 0.4); hi=$(plus_days "$lo" 7)
    note "window $lo .. $hi"
    show "/v1/orders?since=$lo&until=$hi&limit=3" '{total_count, occurred: [.data[].occurred_at]}'

    hdr "cursor round-trip (the server echoes the position it actually served)"
    local p1 cur
    p1=$(api_get "/v1/orders?limit=3")
    if jq -e .next_cursor >/dev/null 2>&1 <<<"$p1"; then
        cur=$(jq -r .next_cursor <<<"$p1")
        jq -c '{cursor, ids: [.data[].order_id]}' <<<"$p1"
        api_get "/v1/orders?limit=3&cursor=$cur" \
            | jq -c --arg sent "$cur" '{echoed_matches: (.cursor == $sent), ids: [.data[].order_id]}'
        note "if echoed_matches is ever false, you were handed a different page than you asked for"
    else
        note "first page did not come back cleanly -- try again"
    fi

    hdr "a single order, and the same one before it was knowable"
    local oid
    oid=$(api_get "/v1/orders?limit=1" | jq -r '.data[0].order_id // empty')
    if [ -n "$oid" ]; then
        show "/v1/orders/$oid" '.data | {order_id, version, occurred_at, knowledge_time}'
        printf '   %s at as_of=%s -> HTTP %s\n' "$oid" "$SIM_START" \
            "$(api_code "/v1/orders/$oid?as_of=$SIM_START")"
    fi

    hdr "orders known vs labels known"
    local no nl
    no=$(api_get "/v1/orders?limit=1" | jq -r '.total_count // "?"')
    nl=$(api_get "/v1/labels?limit=1" | jq -r '.total_count // "?"')
    printf '   orders %s   labels %s\n' "$no" "$nl"
    note "the gap is not missing data; a label cannot exist before the refund window closes"
    show "/v1/labels?limit=2" '.data'

    hdr "customers are SCD2: one row per validity interval"
    show "/v1/customers?limit=200" '[.data[] | select(.valid_to != null)][0] // "no closed interval on this page"'

    hdr "the drop channel is just files"
    local drop; drop=$(tomlv path)
    case "$drop" in /*) ;; *) drop="$REPO/$drop" ;; esac
    if [ -d "$drop" ]; then
        printf '   %s\n' "$drop"
        printf '   payments: %s files   catalog: %s files\n' \
            "$(find "$drop/payments" -type f 2>/dev/null | wc -l)" \
            "$(find "$drop/catalog" -type f 2>/dev/null | wc -l)"
        local newest
        newest=$(find "$drop/payments" -name '*.csv.gz' -type f 2>/dev/null | sort | tail -1)
        [ -n "$newest" ] && { printf '   %s\n' "$(basename "$newest")"; zcat "$newest" 2>/dev/null | head -3 | sed 's/^/     /'; }
    else
        note "$drop does not exist yet -- the writer creates it on the first simulated-day boundary"
    fi

    hdr "done"
    note "probes are read-only apart from --advance. Nothing here writes to the ledger."
}

case "${1:-}" in
    scratch) shift; cmd_scratch "$@" ;;
    probe)   shift; cmd_probe "$@" ;;
    *)       awk 'NR>1 && /^#/ {sub(/^# ?/, ""); print; next} NR>1 {exit}' "$0" ;;
esac
