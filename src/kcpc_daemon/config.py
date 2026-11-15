"""Load and validate runtime configuration."""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


class FeedConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1)
    url: HttpUrl
    read_timeout_seconds: float = Field(default=60, gt=0)


class RouteConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    competition_id: str = Field(min_length=1)
    community: str = Field(min_length=1)
    event_types: list[str] = Field(default_factory=list)


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str = "127.0.0.1"
    port: int = Field(default=8080, ge=1, le=65535)
    database_path: Path = Path("data/daemon.sqlite3")
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    feeds: list[FeedConfig] = Field(default_factory=list)
    routes: list[RouteConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def unique_feed_ids(self) -> "Config":
        if len({feed.id for feed in self.feeds}) != len(self.feeds):
            raise ValueError("feed ids must be unique")
        if len({route.competition_id for route in self.routes}) != len(self.routes):
            raise ValueError("only one route per competition is supported")
        if any(
            route.competition_id not in {feed.id for feed in self.feeds}
            for route in self.routes
        ):
            raise ValueError("route competition_id must match a feed id")
        return self


def load_config(path: Path) -> Config:
    with path.open(encoding="utf-8") as config_file:
        data = yaml.safe_load(config_file)
    if not isinstance(data, dict):
        raise TypeError("configuration must be a YAML mapping")
    return Config.model_validate(data)
