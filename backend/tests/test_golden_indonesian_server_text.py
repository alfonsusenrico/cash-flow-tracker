"""Indonesian server text stays byte-identical (localize-server-text D10, tasks 1.3, 2.3, 3.2, 4.1, 4.2)."""

import json

from app.i18n import t
from tests.golden_server_text import FIXTURE, capture, error_catalogue_texts

GOLDEN = json.loads(FIXTURE.read_text())

# The code each plain-string `detail` of the base revision became (task 3.2).
PLAIN_CODES = {
    'A new account cannot select a default pocket before its child pockets exist': 'default_pocket_before_pockets',
    'A default funding account is only valid for an investment account': 'default_funding_investment_only',
    'No fields to update': 'no_fields_to_update',
    'Account cannot be its own funding account': 'account_own_funding',
    'Account with this name already exists': 'account_name_taken',
    'Account not found': 'account_not_found',
    'Parent account not found': 'parent_account_not_found',
    'Child pockets cannot have sub-pockets (maximum 2 levels allowed)': 'pocket_depth_exceeded',
    'Default funding account not found': 'default_funding_not_found',
    'Default funding account must be liquid': 'default_funding_not_liquid',
    'Default pocket account not found': 'default_pocket_not_found',
    'Clear the default funding account before changing this account to a non-investment type': 'default_funding_blocks_type_change',
    'Clear the default pocket before making this account a child pocket': 'default_pocket_blocks_nesting',
    'Account cannot be its own parent': 'account_own_parent',
    'An account with child pockets cannot become a child pocket': 'parent_cannot_nest',
    'Only a parent account can have a default pocket': 'default_pocket_parent_only',
    'Default pocket must be an active child pocket of this account': 'default_pocket_invalid',
    'Income categories cannot have an expense budget or Kakeibo pillar': 'income_category_budget',
    'Category already exists for this type': 'category_name_taken',
    'Category not found': 'category_not_found',
    'A reimbursement category must stay an income category': 'reimbursement_category_kind',
    'Category type cannot change while reimbursements offset it': 'category_kind_has_offsets',
    'Category type cannot change while incompatible transactions or rules use it': 'category_kind_in_use',
    'A system category of this type already exists': 'system_category_exists',
    'One or more linked accounts were not found': 'linked_accounts_not_found',
    'Adjustment amount must not be zero': 'adjustment_zero',
    'Goal not found': 'goal_not_found',
    'Invalid target_date format, expected YYYY-MM-DD': 'invalid_target_date',
    'Invalid target_date format': 'invalid_target_date',
    'Goal with this name already exists': 'goal_name_taken',
    'Account-backed goal progress is derived from linked account balances': 'goal_progress_derived',
    'Account-backed goal progress cannot be adjusted directly': 'goal_progress_not_adjustable',
    'Goal progress cannot be negative': 'goal_progress_negative',
    'Not found': 'not_found',
    'Notification event not found': 'event_not_found',
    'Alias not found': 'alias_not_found',
    'Sender alias not found': 'sender_alias_not_found',
    'Only unresolved notifications can be retried': 'event_not_retryable',
    'AI processing unavailable': 'ai_unavailable',
    'Supply a non-null amount, date, or notes correction': 'correction_required',
    'Source and target accounts must be different': 'same_account',
    'Effective source pocket not found': 'source_pocket_not_found',
    'Effective target pocket not found': 'target_pocket_not_found',
    'Movement not found': 'movement_not_found',
    'Source account not found': 'source_account_not_found',
    'Either source_account_id or source_account_name must be provided': 'source_account_required',
    'Target account not found': 'target_account_not_found',
    'Either target_account_id or target_account_name must be provided': 'target_account_required',
    'Remaining amount cannot exceed total amount': 'remaining_exceeds_total',
    'Invalid due_date format, expected YYYY-MM-DD': 'invalid_due_date',
    'Invalid due_date format': 'invalid_due_date',
    'Obligation with this name already exists': 'obligation_name_taken',
    'Obligation not found': 'obligation_not_found',
    'Hapus atau pindahkan pembayaran yang terkait sebelum menghapus tagihan ini secara permanen.': 'obligation_has_payments',
    'Schedule day is required': 'schedule_day_required',
    'Source account is required': 'source_account_required',
    'Investment rule account not found': 'account_not_found',
    'Transfer requires target_account_id': 'target_account_required',
    'Cannot transfer to the same account': 'same_account',
    'Transfer categories are assigned by the movement service': 'transfer_category_not_allowed',
    'Target account is only valid for transfers': 'target_account_transfer_only',
    'Category is incompatible with recurring type': 'category_type_mismatch',
    'Category is required for income and expense rules': 'category_required',
    'An obligation can only be linked to an expense rule': 'obligation_expense_only',
    'Rule name is required': 'rule_name_required',
    'Recurring rule not found': 'plan_not_found',
    'Upload receipts through the owned transaction receipt endpoint': 'receipt_upload_endpoint',
    'review_status must be reviewed or needs_review': 'invalid_review_status',
    'A target account is only valid for an investment trade': 'target_account_trade_only',
    'Investment positions can only be changed through a Beli/Jual trade': 'investment_trade_required',
    'Use the movement endpoint for account transfers': 'movement_endpoint_required',
    'Transaction not found': 'transaction_not_found',
    'Linked movements must be edited through the movement endpoint': 'linked_movement_endpoint',
    'Linked movements must be deleted through the movement endpoint': 'linked_movement_endpoint',
    'Either account_id or account_name must be provided': 'account_required',
    'Category is incompatible with transaction type': 'category_type_mismatch',
    'Merchant, review status and offset category are not supported for investment trades': 'trade_fields_not_allowed',
    'Investment trades require a target account': 'trade_target_required',
    'Funding and investment accounts must be different': 'distinct_accounts_required',
    'Trade amount must equal units multiplied by price per unit': 'trade_amount_mismatch',
    'Funding account must be a liquid account': 'funding_not_liquid',
    'Position account must be an investment account': 'position_not_investment',
    'Not authenticated': 'not_authenticated',
    'Investment top-up not found': 'investment_topup_not_found',
    'Funding account not found': 'funding_account_not_found',
    'Suggested product not found': 'product_not_found',
    'Choose two different transactions': 'same_transaction',
    'Choose one outgoing and one incoming transaction': 'movement_sides_required',
    'Both transactions must have the same positive amount': 'movement_amount_mismatch',
    'Accounts must be different': 'same_account',
    'A linked, scheduled, goal, debt, or transfer-fee transaction cannot be merged': 'transaction_not_mergeable',
    'Both accounts must be liquid, or one may be a credit account': 'account_not_liquid',
    'A leg is already linked or was removed': 'leg_unavailable',
    'The legs changed since they were matched': 'legs_changed',
    'Only unresolved notifications can be recorded manually': 'event_not_recordable',
    'Notifikasi ini tidak menunggu konfirmasi rekening': 'source_confirmation_not_pending',
    'Unsupported receipt type. Allowed: pdf, png, jpg, jpeg': 'receipt_type_unsupported',
    'receipt file is empty': 'receipt_empty',
    'Invalid receipt storage path': 'receipt_path_invalid',
    'Receipt image dimensions are too large': 'receipt_dimensions_too_large',
    'Invalid image file': 'receipt_image_invalid',
}


def test_composed_text_is_byte_identical_for_an_indonesian_account():
    current = capture("id", "ID")
    assert current["rendered"] == GOLDEN["rendered"]
    assert current["inline"] == GOLDEN["inline"]


def test_indonesian_prompt_renderings_are_byte_identical():
    assert capture()["prompts"] == GOLDEN["prompts"]


def test_every_indonesian_error_text_of_the_base_is_kept():
    """Where the base wrote Indonesian, the Indonesian catalogue holds exactly those bytes for that code."""
    indonesian = error_catalogue_texts("id")
    site_messages = indonesian["*"]
    english_at_base = set()
    for entry in GOLDEN["errors"]["structured"]:
        code, text = entry["code"], entry["text"]
        if text not in indonesian.get(code, set()) | site_messages:
            english_at_base.add(code)
    # Exactly the codes whose base message was English gain a new Indonesian message; every other is kept.
    assert english_at_base == {
        "account_has_pockets", "account_not_liquid", "already_linked", "amount_product_required",
        "counterpart_candidates", "counterpart_side_mismatch", "investment_trade_required",
        "not_awaiting_counterpart", "same_account", "transaction_not_completable", "unit_mutual_fund_required",
    }
    for entry in GOLDEN["errors"]["plain"]:
        code = PLAIN_CODES[entry["text"]]
        assert code in indonesian, entry["text"]
    for text in ("Hapus atau pindahkan pembayaran yang terkait sebelum menghapus tagihan ini secara permanen.",
                 "Notifikasi ini tidak menunggu konfirmasi rekening"):
        assert t("id", f"errors.{PLAIN_CODES[text]}") == text


def test_english_plain_texts_become_the_english_messages():
    """Today's English plain strings are the English messages, except where several merged into one code."""
    english = error_catalogue_texts("en")
    merged = {text for text, code in PLAIN_CODES.items() if text not in english[code] and text in ENGLISH_MERGED}
    for entry in GOLDEN["errors"]["plain"]:
        text = entry["text"]
        code = PLAIN_CODES[text]
        if text in english[code]:
            continue
        assert text in merged or t("id", f"errors.{code}") == text, text


# English plain strings whose code carries a sibling's wording (one message per code, D4).
ENGLISH_MERGED = {
    "Invalid target_date format",
    "Invalid due_date format",
    "Source account is required",
    "Investment rule account not found",
    "Transfer requires target_account_id",
    "Cannot transfer to the same account",
    "Category is incompatible with recurring type",
    "Investment positions can only be changed through a Beli/Jual trade",
    "Linked movements must be deleted through the movement endpoint",
    "Accounts must be different",
    "Both accounts must be liquid, or one may be a credit account",
    "Suggested product not found",
    "Not found",
}
