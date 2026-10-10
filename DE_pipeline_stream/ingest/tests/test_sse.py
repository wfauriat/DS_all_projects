from bikeshare_bridge.sse import KEEPALIVE, SseFrame, parse


def test_frames_carry_id_event_and_data():
    lines = ["id: 7", "event: trip_started", 'data: {"a": 1}', ""]
    assert list(parse(lines)) == [SseFrame("trip_started", '{"a": 1}', "7")]


def test_multiline_data_is_joined_with_newlines():
    assert list(parse(["data: one", "data: two", ""])) == [SseFrame("message", "one\ntwo")]


def test_comments_become_keepalive_frames():
    assert list(parse([": keepalive", ""])) == [SseFrame(KEEPALIVE, "keepalive")]


def test_value_without_space_after_colon():
    assert next(parse(["data:x", ""])).data == "x"


def test_an_unfinished_frame_is_not_emitted():
    assert list(parse(["id: 1", "data: {}"])) == []  # no blank line yet
