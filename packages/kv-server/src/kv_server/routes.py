"""HTTP routes for the KV store.

Values travel over the wire as UTF-8 strings in JSON bodies. Keys travel
as URL path segments. Both get encoded to bytes at the boundary here --
storage-core's KVStore only ever sees bytes, it has no notion of JSON.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from storage_core.kv.store import KVStore

router = APIRouter()


class SetValueRequest(BaseModel):
    value: str


class ValueResponse(BaseModel):
    key: str
    value: str


class KeysResponse(BaseModel):
    keys: list[str]
    count: int


def _store(request: Request) -> KVStore:
    return request.app.state.store  # type: ignore[no-any-return]


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/keys/{key}", response_model=ValueResponse)
def get_value(key: str, request: Request) -> ValueResponse:
    store = _store(request)
    value = store.get(key.encode("utf-8"))
    if value is None:
        raise HTTPException(status_code=404, detail=f"key '{key}' not found")
    return ValueResponse(key=key, value=value.decode("utf-8"))


@router.put("/keys/{key}", response_model=ValueResponse, status_code=200)
def set_value(key: str, body: SetValueRequest, request: Request) -> ValueResponse:
    store = _store(request)
    store.set(key.encode("utf-8"), body.value.encode("utf-8"))
    return ValueResponse(key=key, value=body.value)


@router.delete("/keys/{key}", status_code=204)
def delete_value(key: str, request: Request) -> None:
    store = _store(request)
    existed = store.delete(key.encode("utf-8"))
    if not existed:
        raise HTTPException(status_code=404, detail=f"key '{key}' not found")


@router.get("/keys", response_model=KeysResponse)
def list_keys(request: Request) -> KeysResponse:
    store = _store(request)
    keys = [k.decode("utf-8") for k in store.keys()]
    return KeysResponse(keys=keys, count=len(keys))


@router.post("/compact", status_code=200)
def compact(request: Request) -> dict[str, str]:
    store = _store(request)
    store.compact()
    return {"status": "compacted"}
