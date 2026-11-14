import pytest

from kcpc_daemon.config import Config, FeedConfig, RouteConfig
from kcpc_daemon.events import FeedEvent, normalize, route


def test_normalization_and_routing() -> None:
    event = FeedEvent(type="submissions", id="s1", data={"team_id": "t1"}, token="1")
    normalized = normalize("contest", event)
    assert normalized is not None
    assert normalized.competition_id == "contest"
    assert normalized.source == "code_court"
    assert normalized.object_id == "s1"
    assert normalized.data == {"team_id": "t1"}
    assert normalize("contest", event) == normalized
    assert normalize("other", event).event_id != normalized.event_id  # type: ignore[union-attr]
    routes = [
        RouteConfig(
            competition_id="contest", community="club", event_types=["submissions"]
        )
    ]
    assert route(normalized, routes) == ["club"]
    assert (
        route(
            normalized,
            [
                RouteConfig(
                    competition_id="contest", community="club", event_types=["teams"]
                )
            ],
        )
        == []
    )
    assert normalize("contest", event.model_copy(update={"type": "unknown"})) is None


def test_routes_require_known_unique_competitions() -> None:
    feed = FeedConfig(id="contest", url="https://example.com/feed")
    route_config = RouteConfig(competition_id="contest", community="club")
    with pytest.raises(ValueError, match="unique"):
        Config(feeds=[feed], routes=[route_config, route_config])
    with pytest.raises(ValueError, match="match a feed"):
        Config(routes=[route_config])


def test_community_filters_are_independent() -> None:
    event = normalize("contest", FeedEvent(type="teams", id="t", data={}, token="1"))
    assert event is not None
    routes = [
        RouteConfig(competition_id="contest", community="all"),
        RouteConfig(competition_id="contest", community="teams", event_types=["teams"]),
        RouteConfig(
            competition_id="contest",
            community="submissions",
            event_types=["submissions"],
        ),
    ]
    config = Config(
        feeds=[FeedConfig(id="contest", url="https://example.com/feed")], routes=routes
    )
    assert route(event, config.routes) == ["all", "teams"]
