from __future__ import annotations

import json

from xtr_messenger.data_collector import DispatchedMessage


def test_a_dispatch_is_kept_as_json_safe_plain_data() -> None:
    dispatched = DispatchedMessage(
        message_class="app.DoWork",
        message={"kind": "scalar", "type": "int", "value": 1},
        stamps_in=("DelayStamp",),
        stamps_out=("DelayStamp", "HandledStamp"),
        handled=({"handler": "app.handle", "result": {"kind": "scalar", "value": None}},),
        sent=({"sender": "app.Sender", "alias": "jobs"},),
        duration_ms=1.5,
    )

    data = dispatched.to_data()

    assert data == {
        "message_class": "app.DoWork",
        "message": {"kind": "scalar", "type": "int", "value": 1},
        "stamps_in": ["DelayStamp"],
        "stamps_out": ["DelayStamp", "HandledStamp"],
        "handled": [{"handler": "app.handle", "result": {"kind": "scalar", "value": None}}],
        "sent": [{"sender": "app.Sender", "alias": "jobs"}],
        "duration_ms": 1.5,
        "error": None,
    }
    assert json.loads(json.dumps(data)) == data


def test_a_dispatch_still_running_has_no_duration_nor_outcome() -> None:
    dispatched = DispatchedMessage(message_class="app.DoWork", message={}, stamps_in=())

    data = dispatched.to_data()

    assert data["duration_ms"] is None
    assert data["error"] is None
    assert (data["handled"], data["sent"]) == ([], [])
