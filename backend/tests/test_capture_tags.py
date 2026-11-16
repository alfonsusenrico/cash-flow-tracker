"""Tags in AI text capture (transaction-tags D4): read by the model, checked, recorded on send, removed by undo."""

import json
from uuid import uuid4

from app.services.capture_normalization import CaptureNormalization
from app.services.capture_resolution import has_tag_wording
from test_notification_processing_database import notification_owner  # noqa: F401
from test_text_capture import (  # noqa: F401
    NO_TIME, captured, capture_rows, owner, provider_with, query, remember, use_provider,
)


def tagged_result(owner, tags, *, description="Bayar makan di Kedai Contoh", category="makan", kakeibo="need"):
    return CaptureNormalization.model_validate_json(json.dumps({
        "merchant": None, "description": description, "type": "expense", "type_confidence": 0.95,
        "category_id": owner[category], "kakeibo": kakeibo, "category_confidence": 0.92,
        "account_id": owner["cash"], "account_confidence": 0.9, "time": NO_TIME, "tags": tags,
    }))


def make_tag(client, name):
    response = client.post("/api/tags", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()["tag"]


def tags_of(transaction_id):
    return sorted(row["name"] for row in query(
        """SELECT tag.name FROM transaction_tags link JOIN tags tag ON tag.id = link.tag_id
           WHERE link.transaction_id = %s""", (transaction_id,)))


def tag_names(owner):
    return sorted(row["name"] for row in query("SELECT name FROM tags WHERE user_id = %s", (owner["user_id"],)))


def test_tag_wording_is_structural():
    assert has_tag_wording("fellow coach: bayar makan di kedai contoh 45rb")
    assert has_tag_wording("isi bensin 150rb #liburan")
    assert has_tag_wording("isi bensin 150rb tag-nya Malang trip")
    assert not has_tag_wording("es teh manis 5rb tunai")
    # A clock time is not a label before a colon.
    assert not has_tag_wording("makan siang 25rb jam 12:30")


def test_a_prefix_maps_to_the_owners_existing_tag(client, owner, monkeypatch):
    fellowship = make_tag(client, "fellowship coach")
    provider = use_provider(monkeypatch, provider_with(tagged_result(
        owner, [{"tag_id": fellowship["id"], "new_name": None, "span": "fellow coach"}])))
    capture = captured(client, "fellow coach: bayar makan di kedai contoh 45rb tunai jam 19")
    assert capture["status"] == "recorded"
    row = capture_rows(owner)[0]
    assert tags_of(row["id"]) == ["fellowship coach"]
    assert row["name"] == "Bayar makan di Kedai Contoh"
    assert capture["transaction"]["tags"] == [{"id": fellowship["id"], "name": "fellowship coach"}]
    context = provider.normalize_capture.await_args.args[0]
    assert {"id": fellowship["id"], "name": "fellowship coach"} in context["tags"]


def test_a_new_tag_on_instruction_is_created_and_undo_removes_it(client, owner, monkeypatch):
    use_provider(monkeypatch, provider_with(tagged_result(
        owner, [{"tag_id": None, "new_name": "Malang trip", "span": "Malang trip"}], description="Isi bensin")))
    capture = captured(client, "isi bensin 150rb tunai tag-nya Malang trip")
    row = capture_rows(owner)[0]
    assert tags_of(row["id"]) == ["Malang trip"]
    undone = client.post(f"/api/capture/{capture['id']}/undo")
    assert undone.status_code == 200, undone.text
    assert tag_names(owner) == []


def test_undo_keeps_a_created_tag_another_transaction_carries(client, owner, monkeypatch):
    use_provider(monkeypatch, provider_with(tagged_result(
        owner, [{"tag_id": None, "new_name": "Malang trip", "span": "Malang trip"}], description="Isi bensin")))
    capture = captured(client, "isi bensin 150rb tunai tag-nya Malang trip")
    tag_id = capture["transaction"]["tags"][0]["id"]
    other = client.post("/api/transactions", json={
        "type": "expense", "amount": 20_000, "account_id": owner["cash"], "category_id": owner["makan"],
        "name": "Makan malam", "tag_ids": [tag_id],
    })
    assert other.status_code == 200, other.text
    assert client.post(f"/api/capture/{capture['id']}/undo").status_code == 200
    assert tag_names(owner) == ["Malang trip"]


def test_an_existing_tag_attached_by_a_capture_survives_its_undo(client, owner, monkeypatch):
    fellowship = make_tag(client, "fellowship coach")
    use_provider(monkeypatch, provider_with(tagged_result(
        owner, [{"tag_id": fellowship["id"], "new_name": None, "span": "fellow coach"}])))
    capture = captured(client, "fellow coach: bayar makan di kedai contoh 45rb tunai")
    assert client.post(f"/api/capture/{capture['id']}/undo").status_code == 200
    assert tag_names(owner) == ["fellowship coach"]


def test_unproven_tags_are_dropped_and_a_duplicate_name_reuses_the_tag(client, owner, monkeypatch):
    fellowship = make_tag(client, "fellowship coach")
    use_provider(monkeypatch, provider_with(tagged_result(owner, [
        {"tag_id": str(uuid4()), "new_name": None, "span": "fellow coach"},
        {"tag_id": None, "new_name": "Liburan", "span": "liburan bali"},
        {"tag_id": None, "new_name": "Fellowship  Coach", "span": "fellow coach"},
    ])))
    capture = captured(client, "fellow coach: bayar makan di kedai contoh 45rb tunai")
    assert capture["status"] == "recorded"
    assert tags_of(capture_rows(owner)[0]["id"]) == ["fellowship coach"]
    assert tag_names(owner) == ["fellowship coach"]
    assert fellowship["id"] == capture["transaction"]["tags"][0]["id"]


def test_text_without_tag_wording_sends_no_tags_to_the_model(client, owner, monkeypatch):
    make_tag(client, "fellowship coach")
    provider = use_provider(monkeypatch, provider_with(tagged_result(
        owner, [], description="Es teh manis", category="jajan", kakeibo="want")))
    capture = captured(client, "es teh manis 5rb tunai")
    assert capture["transaction"]["tags"] == []
    assert provider.normalize_capture.await_args.args[0]["tags"] == []


def test_tag_wording_asks_the_model_even_when_memory_knows_the_merchant(client, owner, monkeypatch):
    remember(owner, "merchant:kedai contoh", category="makan", account="cash", notes="Makan di Kedai Contoh")
    fellowship = make_tag(client, "fellowship coach")
    provider = use_provider(monkeypatch, provider_with(tagged_result(
        owner, [{"tag_id": fellowship["id"], "new_name": None, "span": "fellow coach"}])))
    capture = captured(client, "fellow coach: kedai contoh 45rb tunai")
    assert provider.normalize_capture.await_count == 1
    assert tags_of(capture_rows(owner)[0]["id"]) == ["fellowship coach"]
