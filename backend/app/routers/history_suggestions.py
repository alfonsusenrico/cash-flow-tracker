"""History re-enrichment: owner-started suggestion runs and the owner's accept or reject decisions."""

from __future__ import annotations

from datetime import date
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.db.pool import db_conn
from app.services import history_suggestions as history
from app.services.auth import get_current_user
from app.services.notification_processing import database_operation

router = APIRouter(prefix="/review/suggestions", tags=["Review"])

DECISION_LIMIT = 200


class SuggestionRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    from_date: date
    to_date: date
    confirm: bool = False

    @model_validator(mode="after")
    def bounded_window(self) -> "SuggestionRunRequest":
        history.validate_window(self.from_date, self.to_date)
        return self


class SuggestionDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    suggestion_ids: list[UUID] = Field(min_length=1, max_length=DECISION_LIMIT)

    @model_validator(mode="after")
    def unique_ids(self) -> "SuggestionDecision":
        if len(set(self.suggestion_ids)) != len(self.suggestion_ids):
            raise ValueError("suggestion_ids must not repeat")
        return self


def http_error(error: history.SuggestionError) -> HTTPException:
    return HTTPException(status_code=error.status, detail=error.detail)


def provider_of(request: Request):
    return getattr(request.app.state, "notification_provider", None)


def estimate_window(user_id: str, payload: SuggestionRunRequest, provider) -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            result = history.estimate(cur, user_id, payload.from_date, payload.to_date, provider)
        conn.commit()
    return result


def start_run(user_id: str, payload: SuggestionRunRequest, provider) -> tuple[dict, dict]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            result = history.estimate(cur, user_id, payload.from_date, payload.to_date, provider)
            run = history.create_run(cur, user_id, payload.from_date, payload.to_date, provider)
        conn.commit()
    return result, run


@router.post("/runs")
async def create_suggestion_run(
    payload: SuggestionRunRequest, request: Request, background: BackgroundTasks,
    current_user: dict = Depends(get_current_user),
):
    """Without `confirm` only the estimate is returned; with it the run starts in the background."""
    configuration_error = history.settings.history_suggestion_configuration_error
    if configuration_error:
        raise HTTPException(status_code=503, detail={
            "code": configuration_error,
            "message": "Pengaturan saran riwayat tidak valid.",
        })
    user_id = str(current_user["id"])
    provider = provider_of(request)
    if not payload.confirm:
        return {"ok": True, "estimate": await database_operation(estimate_window, user_id, payload, provider)}
    try:
        estimate, run = await database_operation(start_run, user_id, payload, provider)
    except history.SuggestionError as error:
        raise http_error(error) from None
    background.add_task(history.execute_run, str(run["id"]), str(run["lease_token"]), user_id, provider)
    return {"ok": True, "estimate": estimate, "run": history.run_view(run)}


@router.get("/runs/latest")
def latest_suggestion_run(current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        with conn.cursor() as cur:
            run = history.latest_run(cur, str(current_user["id"]))
        conn.commit()
    return {"ok": True, "run": history.run_view(run)}


@router.get("")
def list_suggestions(current_user: dict = Depends(get_current_user)):
    return {"ok": True, **history.pending_suggestions(str(current_user["id"]))}


@router.post("/accept")
def accept_suggestions(payload: SuggestionDecision, current_user: dict = Depends(get_current_user)):
    try:
        result = history.accept_suggestions(str(current_user["id"]), [str(value) for value in payload.suggestion_ids])
    except history.SuggestionError as error:
        raise http_error(error) from None
    return {"ok": True, **result}


@router.post("/reject")
def reject_suggestions(payload: SuggestionDecision, current_user: dict = Depends(get_current_user)):
    try:
        result = history.reject_suggestions(str(current_user["id"]), [str(value) for value in payload.suggestion_ids])
    except history.SuggestionError as error:
        raise http_error(error) from None
    return {"ok": True, **result}
