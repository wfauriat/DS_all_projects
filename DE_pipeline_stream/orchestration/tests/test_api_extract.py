"""API extracts against a scripted source: snapshots replace, incremental loads only add."""

import httpx

from landing import api_extract

STATIONS = [
    {"station_id": "ST-001", "name": "Old Mill", "lat": 45.0, "lon": 5.0, "capacity": 20,
     "zone": "business", "installed_at": "2025-01-01T00:00:00Z", "updated_at": "2025-01-01T00:00:00Z"},
]  # fmt: skip


def weather(hour: int) -> dict:
    return {
        "observed_at": f"2026-03-02T{hour:02d}:00:00Z",
        "temperature_c": 8.5,
        "precipitation_mm": 0.0,
        "wind_kmh": 12.0,
    }


def fault(fault_id: int) -> dict:
    return {"fault_id": fault_id, "fault_type": "duplicate", "injected_at": "2026-03-02T05:00:10Z",
            "event_id": f"e{fault_id}", "event_type": "trip_started", "details": {"x": 1}}  # fmt: skip


def api(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler), base_url="http://source")


def test_snapshots_replace_instead_of_accumulating(db):
    client = api(lambda request: httpx.Response(200, json=STATIONS))
    api_extract.extract_stations(db, client, run_id="r1")
    api_extract.extract_stations(db, client, run_id="r2")
    rows = db.execute("SELECT station_id, capacity, installed_at FROM raw.stations").fetchall()
    assert len(rows) == 1 and rows[0][:2] == ("ST-001", 20)


def test_weather_asks_only_for_what_is_newer(db):
    requests = []
    served = [[weather(5), weather(6)], [weather(7)]]

    def handler(request):
        requests.append(request.url.params["start"])
        return httpx.Response(200, json=served.pop(0))

    client = api(handler)
    assert api_extract.extract_weather(db, client, run_id="r1") == 2
    assert api_extract.extract_weather(db, client, run_id="r2") == 1
    assert requests[1].startswith("2026-03-02T07:00:00")  # latest held (06:00) + 1 hour


def test_fault_log_pages_until_exhausted_and_never_duplicates(db, monkeypatch):
    monkeypatch.setattr(api_extract, "FAULT_LOG_PAGE", 2)
    log = [fault(i) for i in range(1, 6)]

    def handler(request):
        after, limit = int(request.url.params["after_id"]), int(request.url.params["limit"])
        return httpx.Response(200, json=log[after : after + limit])

    client = api(handler)
    assert api_extract.extract_fault_log(db, client, run_id="r1") == 5  # pages of 2, 2, 1
    assert api_extract.extract_fault_log(db, client, run_id="r2") == 0  # nothing newer
    details = db.execute("SELECT details FROM raw.fault_log WHERE fault_id = 3").fetchone()[0]
    assert details == '{"x": 1}'
