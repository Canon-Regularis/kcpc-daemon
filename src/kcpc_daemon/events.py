"""Code Court event normalization and community routing."""

import hashlib
import json
from typing import Literal

from pydantic import BaseModel, ConfigDict, StrictStr

from kcpc_daemon.config import RouteConfig


class FeedEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: StrictStr
    id: StrictStr
    data: dict[str, object] | None
    token: StrictStr


CODE_COURT_TYPES = frozenset(
    {"contests", "state", "problems", "teams", "submissions", "judgements", "awards"}
)


class NormalizedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str
    competition_id: str
    source: Literal["code_court"] = "code_court"
    type: str
    object_id: str
    data: dict[str, object] | None


def normalize(competition_id: str, event: FeedEvent) -> NormalizedEvent | None:
    if event.type not in CODE_COURT_TYPES:
        return None
    identity = json.dumps([competition_id, event.token], ensure_ascii=False).encode()
    return NormalizedEvent(
        event_id=hashlib.sha256(identity).hexdigest(),
        competition_id=competition_id,
        type=event.type,
        object_id=event.id,
        data=event.data,
    )


def route(event: NormalizedEvent, routes: list[RouteConfig]) -> str | None:
    for item in routes:
        if item.competition_id == event.competition_id and (
            not item.event_types or event.type in item.event_types
        ):
            return item.community
    return None
