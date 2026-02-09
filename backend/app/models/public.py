from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class PublicRegisterRequest(BaseModel):
    username: str
    password: str
    full_name: str | None = None
    invite_code: str


class PublicRegisterResponse(BaseModel):
    ok: bool = True
    username: str
    full_name: str
    api_key: str


class EmptyBodyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CreateApiKeyResponse(BaseModel):
    ok: bool = True
    api_key: str


class ApiKeyMetadata(BaseModel):
    api_key_id: str
    key_masked: str
    created_at: datetime
    last_used_at: datetime | None = None


class ApiKeyInfoResponse(BaseModel):
    api_key: ApiKeyMetadata


class ApiKeyResetResponse(BaseModel):
    ok: bool = True
    api_key: str
    masked: str


class AccountCreateRequest(BaseModel):
    account_name: str = Field(min_length=1)
    initial_balance: int = Field(default=0, ge=0)
    monthly_limit: int | None = Field(default=None, ge=0)


class TransactionUpsertRequest(BaseModel):
    transaction_id: str | None = None
    account_id: str | None = None
    transaction_type: Literal["debit", "credit"] | None = None
    transaction_name: str | None = None
    amount: int | None = Field(default=None, gt=0)
    date: str | None = None


class LedgerListRequest(BaseModel):
    scope: Literal["all", "account"] = "all"
    account_id: str | None = None
    from_date: str | None = None
    to_date: str | None = None
    limit: int = Field(default=25, ge=1, le=100)
    cursor: str | None = None
    order: Literal["asc", "desc"] = "desc"
    q: str | None = None


class PublicTransactionItem(BaseModel):
    transaction_id: str
    account_id: str
    account_name: str
    date: datetime
    transaction_name: str
    debit: int
    credit: int
    balance: int
    is_transfer: bool = False
    transfer_id: str | None = None


class CursorLedgerResponse(BaseModel):
    scope: str
    range: dict
    rows: list[dict]
    paging: dict


class PeriodQuery(BaseModel):
    month: str | None = None
    year: str | None = None
