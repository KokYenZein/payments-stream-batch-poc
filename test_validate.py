"""unit tests for the validation rules in the stream consumer."""
import json

from stream_consumer import validate

GOOD = {
    "event_id": "e-1",
    "account_id": "ACC0001",
    "merchant_category": "groceries",
    "amount": 12.5,
    "currency": "MYR",
    "event_time": "2026-11-22T00:00:00+00:00",
}


def encode(**changes):
    return json.dumps({**GOOD, **changes}).encode()


def test_valid_event_is_accepted():
    event, reason = validate(encode())
    assert reason is None
    assert event["event_id"] == "e-1"


def test_negative_amount_is_rejected():
    event, reason = validate(encode(amount=-5))
    assert event is None
    assert reason == "amount must be a positive number"


def test_missing_account_is_rejected():
    event, reason = validate(encode(account_id=None))
    assert event is None
    assert reason == "missing account_id"


def test_wrong_currency_is_rejected():
    event, reason = validate(encode(currency="USD"))
    assert event is None
    assert reason == "unexpected currency"


def test_broken_json_is_rejected():
    event, reason = validate(b"{not json")
    assert event is None
    assert reason == "not valid json"


def test_amount_as_text_is_rejected():
    event, reason = validate(encode(amount="12.50"))
    assert event is None
    assert reason == "amount must be a positive number"
