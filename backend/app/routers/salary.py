"""Authenticated owner controls for confirmed salary receipts."""

from uuid import UUID
from typing import Any, Callable
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict

from app.db.pool import db_conn
from app.services.auth import get_current_user
from app.services import salary_receipts, salary_cycles
from app.core.config import settings

router = APIRouter(tags=['Salary'])


class ReceiptRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    transaction_id: UUID


def _execute(operation: Callable[..., Any], user_id: str, *args: Any) -> Any:
    try:
        with db_conn() as conn:
            return operation(conn, user_id, *args)
    except salary_receipts.SalaryError as error:
        raise HTTPException(status_code=error.status, detail={'code': error.code, 'message': error.message}) from error


@router.get('/source')
def salary_source(current_user: dict = Depends(get_current_user)):
    return {'ok': True, 'source': _execute(salary_receipts.get_source, current_user['id'])}


@router.put('/source')
def set_salary_source(payload: ReceiptRequest, current_user: dict = Depends(get_current_user)):
    return {'ok': True, 'source': _execute(salary_receipts.configure_source, current_user['id'], str(payload.transaction_id))}


@router.get('/candidates')
def salary_candidates(current_user: dict = Depends(get_current_user)):
    return {'ok': True, 'candidates': _execute(salary_receipts.candidates, current_user['id'])}


@router.get('/invalidated-detections')
def salary_invalidated_detections(current_user: dict = Depends(get_current_user)):
    return {'ok': True, 'detections': _execute(salary_receipts.invalidated_detections, current_user['id'])}


@router.get('/receipts')
def salary_receipts_list(current_user: dict = Depends(get_current_user)):
    return {'ok': True, 'receipts': _execute(salary_receipts.receipts, current_user['id'])}


@router.post('/receipts')
def confirm_salary(payload: ReceiptRequest, current_user: dict = Depends(get_current_user)):
    return {'ok': True, 'changed': _execute(salary_receipts.confirm, current_user['id'], str(payload.transaction_id))}


@router.post('/reject')
def reject_salary(payload: ReceiptRequest, current_user: dict = Depends(get_current_user)):
    return {'ok': True, 'changed': _execute(salary_receipts.reject, current_user['id'], str(payload.transaction_id))}


@router.delete('/receipts/{transaction_id}')
def unconfirm_salary(transaction_id: UUID, current_user: dict = Depends(get_current_user)):
    return {'ok': True, 'changed': _execute(salary_receipts.revoke, current_user['id'], str(transaction_id))}


@router.get('/cycle')
def salary_cycle(offset: int = 0, current_user: dict = Depends(get_current_user)):
    rows = _execute(salary_receipts.receipts, current_user['id'])
    resolution = salary_cycles.resolve_cycle([row['date'] for row in rows], offset=offset, tz_name=settings.tz)
    return {
        'ok': True, 'status': resolution.status, 'start': resolution.start,
        'end_exclusive': resolution.end_exclusive, 'timezone': settings.tz,
    }


@router.post('/detect')
def detect_salary(current_user: dict = Depends(get_current_user)):
    return {'ok': True, **_execute(salary_receipts.detect_for_user, current_user['id'])}
