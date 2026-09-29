from datetime import datetime

import pytest

from app.services.notification_evidence import collect_facts
from app.services.notification_resolution import is_external_counterparty, names_owner
from tests.fixtures.real_notifications import OWNER_ALIAS, REAL_NOTIFICATIONS

POST_TIME = datetime.fromisoformat("2026-09-29T13:08:51+07:00")


@pytest.mark.parametrize("label,package,title,text,status,direction,amount", REAL_NOTIFICATIONS,
                         ids=[case[0] for case in REAL_NOTIFICATIONS])
def test_real_notification_facts(label, package, title, text, status, direction, amount):
    facts = collect_facts({"package_name": package, "title": title, "body_text": text, "post_time": POST_TIME})
    assert (facts.status, facts.error_code) == (status, None)
    if status == "candidate":
        assert (facts.direction, facts.amount) == (direction, amount)


@pytest.mark.parametrize("label,owner,external", [
    ("mybca-received-third-party", False, True),
    ("mybca-received-masked-owner", True, False),
    ("jago-has-sent-owner", True, False),
    ("shopeepay-bifast-owner", True, False),
    ("shopeepay-top-up", False, False),
    ("mybca-spent-top-up-leg", False, False),
    ("mybca-salary", False, True),
])
def test_real_notification_identity(label, owner, external):
    _, package, title, text, *_ = next(case for case in REAL_NOTIFICATIONS if case[0] == label)
    facts = collect_facts({"package_name": package, "title": title, "body_text": text, "post_time": POST_TIME})
    assert names_owner(text, [OWNER_ALIAS]) is owner
    assert is_external_counterparty(facts.parsed.counterparty, facts.parsed.category_hint, [OWNER_ALIAS]) is external
