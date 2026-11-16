from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.db.pool import db_conn
from app.services.auth import get_current_user
from app.services.tags import create_tag, list_tags, merge_tags, update_tag

router = APIRouter(tags=["Tags"])

# Generous input bounds; the tag rules themselves (1-40 characters) answer with `tag_name_invalid`.
NAME_INPUT_MAX_LENGTH = 200


class TagCreate(BaseModel):
    name: str = Field(max_length=NAME_INPUT_MAX_LENGTH)


class TagUpdate(BaseModel):
    name: str | None = Field(default=None, max_length=NAME_INPUT_MAX_LENGTH)
    archived: bool | None = None


class TagMerge(BaseModel):
    into_id: UUID


@router.get("")
def get_tags(include_archived: bool = False, current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        with conn.cursor() as cur:
            tags = list_tags(cur, current_user["id"], include_archived=include_archived)
    return {"ok": True, "tags": tags}


@router.post("")
def post_tag(payload: TagCreate, current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        with conn.cursor() as cur:
            tag = create_tag(cur, current_user["id"], payload.name)
            conn.commit()
    return {"ok": True, "tag": tag}


@router.patch("/{tag_id}")
def patch_tag(tag_id: UUID, payload: TagUpdate, current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        with conn.cursor() as cur:
            tag = update_tag(cur, current_user["id"], str(tag_id), name=payload.name, archived=payload.archived)
            conn.commit()
    return {"ok": True, "tag": tag}


@router.post("/{tag_id}/merge")
def merge_tag(tag_id: UUID, payload: TagMerge, current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        with conn.cursor() as cur:
            tag = merge_tags(cur, current_user["id"], str(tag_id), str(payload.into_id))
            conn.commit()
    return {"ok": True, "tag": tag}
