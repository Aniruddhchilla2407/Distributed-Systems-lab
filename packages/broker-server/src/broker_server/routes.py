"""HTTP routes for the broker.

Message keys/values travel over the wire as UTF-8 strings in JSON bodies,
same convention as kv-server -- broker-core only ever sees bytes.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from broker_server.registry import BrokerRegistry, TopicAlreadyExistsError, TopicNotFoundError

router = APIRouter()


class CreateTopicRequest(BaseModel):
    name: str
    num_partitions: int = 1


class TopicInfo(BaseModel):
    name: str
    num_partitions: int


class ProduceRequest(BaseModel):
    value: str
    key: str | None = None


class ProduceResponse(BaseModel):
    partition: int
    offset: int


class ConsumeRequest(BaseModel):
    member_id: str
    max_records: int = 100


class ConsumedRecord(BaseModel):
    partition: int
    offset: int
    next_offset: int
    key: str | None
    value: str
    timestamp_ms: int


class ConsumeResponse(BaseModel):
    records: list[ConsumedRecord]
    count: int


def _registry(request: Request) -> BrokerRegistry:
    return request.app.state.registry  # type: ignore[no-any-return]


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/topics", response_model=TopicInfo, status_code=201)
def create_topic(body: CreateTopicRequest, request: Request) -> TopicInfo:
    registry = _registry(request)
    try:
        topic = registry.create_topic(body.name, body.num_partitions)
    except TopicAlreadyExistsError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return TopicInfo(name=topic.name, num_partitions=topic.num_partitions)


@router.get("/topics", response_model=list[TopicInfo])
def list_topics(request: Request) -> list[TopicInfo]:
    registry = _registry(request)
    return [
        TopicInfo(name=t.name, num_partitions=t.num_partitions) for t in registry.list_topics()
    ]


@router.post("/topics/{topic_name}/produce", response_model=ProduceResponse)
def produce(topic_name: str, body: ProduceRequest, request: Request) -> ProduceResponse:
    registry = _registry(request)
    try:
        result = registry.produce(
            topic_name,
            value=body.value.encode("utf-8"),
            key=body.key.encode("utf-8") if body.key is not None else None,
        )
    except TopicNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return ProduceResponse(partition=result.partition, offset=result.offset)


@router.post("/topics/{topic_name}/groups/{group_id}/consume", response_model=ConsumeResponse)
def consume(
    topic_name: str, group_id: str, body: ConsumeRequest, request: Request
) -> ConsumeResponse:
    registry = _registry(request)
    try:
        records = registry.consume(topic_name, group_id, body.member_id, body.max_records)
    except TopicNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return ConsumeResponse(
        records=[
            ConsumedRecord(
                partition=r.partition,
                offset=r.offset,
                next_offset=r.next_offset,
                key=r.key.decode("utf-8") if r.key is not None else None,
                value=r.value.decode("utf-8"),
                timestamp_ms=r.timestamp_ms,
            )
            for r in records
        ],
        count=len(records),
    )