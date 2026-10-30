"""Confirmed salary detection with synthetic transactions only."""
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from test_reimbursements import owner, record  # noqa: F401 - fixture


def income(client, owner, amount, notes, when=None):
    payload = {'type': 'income', 'account_id': owner['cash'], 'category_id': owner['salary'],
               'amount': amount, 'notes': notes}
    if when:
        payload['date'] = when.isoformat()
    response = client.post('/api/transactions', json=payload)
    assert response.status_code == 200, response.text
    return response.json()['transaction_id']


def test_confirmed_source_candidates_require_owner_confirmation(client, owner):
    salary = income(client, owner, 5_000_000, 'Synthetic Employer')
    bonus = income(client, owner, 1_000_000, 'Synthetic Employer')
    other = income(client, owner, 2_000_000, 'Other sender')
    configured = client.put('/api/salary/source', json={'transaction_id': salary})
    assert configured.status_code == 200
    candidates = client.get('/api/salary/candidates').json()['candidates']
    assert {row['id'] for row in candidates} == {salary, bonus}
    assert all(not row['confirmed'] for row in candidates)
    assert client.get('/api/salary/receipts').json()['receipts'] == []
    assert other not in {row['id'] for row in candidates}
    confirmed = client.post('/api/salary/receipts', json={'transaction_id': salary})
    assert confirmed.status_code == 200 and confirmed.json()['changed'] is True
    assert client.post('/api/salary/receipts', json={'transaction_id': salary}).json()['changed'] is False
    conflict = client.post('/api/salary/receipts', json={'transaction_id': bonus})
    assert conflict.status_code == 409 and conflict.json()['detail']['code'] == 'salary_same_day_conflict'
    assert client.delete(f'/api/salary/receipts/{salary}').json()['changed'] is True
    assert client.delete(f'/api/salary/receipts/{salary}').json()['changed'] is False
    assert client.post('/api/salary/receipts', json={'transaction_id': bonus}).json()['changed'] is True
    assert client.get('/api/transactions/' + salary).status_code == 200


def test_salary_rejects_expenses_reimbursements_future_and_foreign_rows(client, owner):
    expense = record(client, owner, 'dinner', 20_000)['transaction_id']
    refund = record(client, owner, 'reimbursement', 10_000, tx_type='income', offset='dinner')['transaction_id']
    future = income(client, owner, 1_000_000, 'Synthetic Employer', datetime.now(timezone.utc) + timedelta(days=1))
    for transaction_id in (expense, refund, future):
        assert client.post('/api/salary/receipts', json={'transaction_id': transaction_id}).status_code == 422
    assert client.post('/api/salary/receipts', json={'transaction_id': str(uuid4())}).status_code == 404
    assert client.put('/api/salary/source', json={'transaction_id': expense}).status_code == 422
    assert client.post('/api/salary/receipts', json={'transaction_id': str(uuid4()), 'extra': True}).status_code == 422


def test_edit_or_delete_of_confirmed_income_does_not_create_a_transaction(client, owner):
    salary = income(client, owner, 5_000_000, 'Synthetic Employer', datetime.now(timezone.utc) - timedelta(days=2))
    assert client.post('/api/salary/receipts', json={'transaction_id': salary}).status_code == 200
    amended = datetime.now(timezone.utc) - timedelta(days=1)
    assert client.patch('/api/transactions/' + salary, json={'date': amended.isoformat()}).status_code == 200
    rows = client.get('/api/salary/receipts').json()['receipts']
    assert len(rows) == 1 and rows[0]['id'] == salary
    assert client.delete('/api/transactions/' + salary).status_code == 200
    assert client.get('/api/salary/receipts').json()['receipts'] == []


def test_concurrent_confirmation_is_idempotent_and_audited_once(client, owner):
    from concurrent.futures import ThreadPoolExecutor
    from app.db.pool import db_conn
    from app.services.salary_receipts import confirm

    salary = income(client, owner, 5_000_000, 'Synthetic Employer')
    def attempt():
        with db_conn() as conn:
            return confirm(conn, owner['user_id'], salary)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: attempt(), range(2)))
    assert sorted(results) == [False, True]
    with db_conn() as conn:
        count = conn.execute("SELECT COUNT(*) AS n FROM salary_receipt_events WHERE user_id = %s AND action = 'confirm'", (owner['user_id'],)).fetchone()['n']
    assert count == 1


def test_a_different_owner_cannot_confirm_or_configure_the_transaction(client, owner):
    from app.db.pool import db_conn
    stranger = str(uuid4())
    account = str(uuid4())
    transaction = str(uuid4())
    with db_conn() as conn:
        conn.execute("INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Synthetic', 'hash', 'TEST')", (stranger, 'foreign_salary_' + uuid4().hex))
        conn.execute("INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, 'Synthetic', 'bank', 0)", (account, stranger))
        conn.execute("INSERT INTO transactions (id, user_id, account_id, type, amount, notes, date, source, review_status) VALUES (%s, %s, %s, 'income', 1000, 'Synthetic', now(), 'manual', 'reviewed')", (transaction, stranger, account))
        conn.commit()
    try:
        assert client.post('/api/salary/receipts', json={'transaction_id': transaction}).status_code == 404
        assert client.put('/api/salary/source', json={'transaction_id': transaction}).status_code == 404
    finally:
        with db_conn() as conn:
            conn.execute('DELETE FROM users WHERE id = %s', (stranger,))
            conn.commit()


def test_owner_can_confirm_a_receipt_not_matched_by_the_source(client, owner):
    example = income(client, owner, 5_000_000, 'Synthetic Employer', datetime.now(timezone.utc) - timedelta(days=40))
    changed_description = income(client, owner, 5_000_000, 'Synthetic Employer - changed description')
    assert client.put('/api/salary/source', json={'transaction_id': example}).status_code == 200
    assert changed_description not in {row['id'] for row in client.get('/api/salary/candidates').json()['candidates']}
    before = client.get('/api/transactions').json()['total']
    assert client.post('/api/salary/receipts', json={'transaction_id': changed_description}).json()['changed'] is True
    assert client.get('/api/transactions').json()['total'] == before
    assert client.get('/api/salary/receipts').json()['receipts'][0]['id'] == changed_description


def test_internal_movement_income_cannot_be_a_salary(client, owner):
    from app.db.pool import db_conn
    target = str(uuid4())
    with db_conn() as conn:
        conn.execute("INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, 'Other synthetic account', 'bank', 0)", (target, owner['user_id']))
        conn.commit()
    movement = client.post('/api/movements', json={
        'source_account_id': owner['cash'], 'target_account_id': target, 'amount': 50_000,
    })
    assert movement.status_code == 200, movement.text
    with db_conn() as conn:
        row = conn.execute("SELECT id FROM transactions WHERE user_id = %s AND account_id = %s AND type = 'income' AND movement_id IS NOT NULL", (owner['user_id'], target)).fetchone()
    transaction_id = str(row['id'])
    assert client.put('/api/salary/source', json={'transaction_id': transaction_id}).status_code == 422
    assert client.post('/api/salary/receipts', json={'transaction_id': transaction_id}).status_code == 422


def test_detected_salary_requires_source_and_explicit_marker(client, owner):
    def marked(notes, days):
        response = client.post('/api/transactions', json={'type': 'income', 'account_id': owner['cash'],
            'category_id': owner['salary'], 'amount': 1_000_000, 'notes': notes,
            'merchant': 'Synthetic Employer', 'date': (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()})
        assert response.status_code == 200, response.text
        return response.json()['transaction_id']
    salary = marked('Gaji bulanan', 60)
    bonus = marked('Salary bonus', 30)
    unclear = marked('Transfer masuk', 10)
    assert client.post('/api/salary/detect').json()['detected'] == 0
    assert client.put('/api/salary/source', json={'transaction_id': salary}).status_code == 200
    detection = client.post('/api/salary/detect').json()
    assert detection['detected'] == 1 and detection['review'] == 2
    receipts = client.get('/api/salary/receipts').json()['receipts']
    assert [(r['id'], r['confirmation_method']) for r in receipts] == [(salary, 'detected')]
    assert {r['id'] for r in client.get('/api/salary/candidates').json()['candidates']} == {salary, bonus, unclear}
    assert client.post('/api/salary/detect').json()['detected'] == 0
    assert client.delete('/api/salary/receipts/' + salary).json()['changed'] is True
    assert client.post('/api/salary/detect').json()['detected'] == 0
    assert client.get('/api/salary/receipts').json()['receipts'] == []


def test_detection_skips_same_day_collisions_and_invalidated_salary(client, owner):
    def marked(notes, days):
        response = client.post('/api/transactions', json={'type': 'income', 'account_id': owner['cash'],
            'category_id': owner['salary'], 'amount': 1_000_000, 'notes': notes,
            'merchant': 'Synthetic Employer', 'date': (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()})
        assert response.status_code == 200, response.text
        return response.json()['transaction_id']
    salary = marked('Gaji bulanan', 60)
    first = marked('Gaji bulanan', 30)
    second = marked('Salary', 30)
    assert client.put('/api/salary/source', json={'transaction_id': salary}).status_code == 200
    detection = client.post('/api/salary/detect').json()
    assert detection['detected'] == 1 and detection['review'] == 2
    assert [row['id'] for row in client.get('/api/salary/receipts').json()['receipts']] == [salary]
    assert client.patch('/api/transactions/' + salary, json={'notes': 'Salary bonus'}).status_code == 200
    assert client.get('/api/salary/receipts').json()['receipts'] == []
    assert client.post('/api/salary/receipts', json={'transaction_id': first}).json()['changed'] is True
    assert client.post('/api/salary/receipts', json={'transaction_id': second}).status_code == 409


def test_detected_receipt_revocation_is_not_undone_by_a_stale_retry(client, owner):
    from app.db.pool import db_conn
    from app.services.salary_receipts import confirm

    salary = income(client, owner, 1_000_000, 'Gaji synthetic')
    assert client.put('/api/salary/source', json={'transaction_id': salary}).status_code == 200
    assert client.post('/api/salary/detect').json()['detected'] == 1
    assert client.delete('/api/salary/receipts/' + salary).json()['changed'] is True
    with db_conn() as conn:
        assert confirm(conn, owner['user_id'], salary, method='detected') is False
    assert client.get('/api/salary/receipts').json()['receipts'] == []


def test_a_later_same_day_candidate_reverts_automatic_confirmation_for_review(client, owner):
    first = income(client, owner, 1_000_000, 'Gaji synthetic')
    assert client.put('/api/salary/source', json={'transaction_id': first}).status_code == 200
    assert client.post('/api/salary/detect').json()['detected'] == 1
    second = income(client, owner, 1_000_000, 'Gaji synthetic')
    result = client.post('/api/salary/detect').json()
    assert result['detected'] == 0 and result['review'] == 2
    assert client.get('/api/salary/receipts').json()['receipts'] == []
    assert client.post('/api/salary/receipts', json={'transaction_id': second}).json()['changed'] is True


def test_marker_rejects_bonus_refund_and_negated_salary_text():
    from app.services.salary_receipts import has_explicit_salary_marker

    assert has_explicit_salary_marker('Synthetic employer', 'Gaji bulanan')
    assert has_explicit_salary_marker('Synthetic employer', 'Monthly salary')
    for note in ('Salary bonus', 'Gaji refund', 'Gaji cashback', 'Bukan gaji', 'Not salary', 'Gaji ganti uang', 'Transfer biasa'):
        assert not has_explicit_salary_marker('Synthetic employer', note)


def test_cycle_api_follows_confirmed_receipts_and_corrections(client, owner):
    assert client.get('/api/salary/cycle').json()['status'] == 'not_configured'
    old_date = datetime.now(timezone.utc) - timedelta(days=45)
    recent_date = datetime.now(timezone.utc) - timedelta(days=10)
    old = income(client, owner, 4_000_000, 'Synthetic salary old', old_date)
    recent = income(client, owner, 5_000_000, 'Synthetic salary recent', recent_date)
    assert client.post('/api/salary/receipts', json={'transaction_id': old}).status_code == 200
    assert client.post('/api/salary/receipts', json={'transaction_id': recent}).status_code == 200
    current = client.get('/api/salary/cycle').json()
    previous = client.get('/api/salary/cycle?offset=-1').json()
    assert current['status'] == 'open' and current['end_exclusive'] is None
    assert datetime.fromisoformat(current['start']) == recent_date
    assert previous['status'] == 'closed'
    assert datetime.fromisoformat(previous['start']) == old_date
    assert datetime.fromisoformat(previous['end_exclusive']) == recent_date
    corrected = recent_date - timedelta(days=2)
    assert client.patch('/api/transactions/' + recent, json={'date': corrected.isoformat()}).status_code == 200
    assert datetime.fromisoformat(client.get('/api/salary/cycle').json()['start']) == corrected
    assert client.delete('/api/transactions/' + recent).status_code == 200
    assert datetime.fromisoformat(client.get('/api/salary/cycle').json()['start']) == old_date


def test_edited_detected_salary_remains_visible_for_review_after_source_changes(client, owner):
    salary = income(client, owner, 1_000_000, 'Gaji synthetic')
    assert client.put('/api/salary/source', json={'transaction_id': salary}).status_code == 200
    assert client.post('/api/salary/detect').json()['detected'] == 1
    assert client.patch('/api/transactions/' + salary, json={'notes': 'Bonus synthetic'}).status_code == 200
    assert client.get('/api/salary/receipts').json()['receipts'] == []
    assert client.get('/api/salary/candidates').json()['candidates'] == []
    review = client.get('/api/salary/invalidated-detections').json()['detections']
    assert [row['id'] for row in review] == [salary]
    assert client.post('/api/salary/receipts', json={'transaction_id': salary}).json()['changed'] is True
    assert client.get('/api/salary/invalidated-detections').json()['detections'] == []


def test_owner_can_reject_ambiguous_income_without_creating_transactions(client, owner):
    salary = income(client, owner, 1_000_000, 'Synthetic employer')
    bonus = income(client, owner, 500_000, 'Synthetic employer', datetime.now(timezone.utc) - timedelta(days=2))
    assert client.put('/api/salary/source', json={'transaction_id': salary}).status_code == 200
    before = client.get('/api/transactions').json()['total']
    assert client.post('/api/salary/reject', json={'transaction_id': bonus}).json()['changed'] is True
    assert client.post('/api/salary/reject', json={'transaction_id': bonus}).json()['changed'] is False
    candidate = next(row for row in client.get('/api/salary/candidates').json()['candidates'] if row['id'] == bonus)
    assert candidate['rejected'] and not candidate['needs_review']
    assert client.post('/api/salary/detect').json()['detected'] == 0
    assert client.get('/api/transactions').json()['total'] == before
    assert client.post('/api/salary/receipts', json={'transaction_id': bonus}).json()['changed'] is True
    assert any(row['id'] == bonus for row in client.get('/api/salary/receipts').json()['receipts'])


def test_reconfirming_invalidated_detection_cannot_override_same_day_owner_salary(client, owner):
    first = income(client, owner, 1_000_000, 'Gaji synthetic')
    assert client.put('/api/salary/source', json={'transaction_id': first}).status_code == 200
    assert client.post('/api/salary/detect').json()['detected'] == 1
    assert client.patch('/api/transactions/' + first, json={'notes': 'Bonus synthetic'}).status_code == 200
    second = income(client, owner, 2_000_000, 'Another salary')
    assert client.post('/api/salary/receipts', json={'transaction_id': second}).status_code == 200
    response = client.post('/api/salary/receipts', json={'transaction_id': first})
    assert response.status_code == 409
    assert [row['id'] for row in client.get('/api/salary/receipts').json()['receipts']] == [second]
