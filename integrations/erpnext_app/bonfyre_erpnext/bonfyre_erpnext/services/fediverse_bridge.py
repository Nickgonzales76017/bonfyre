"""Bridge to a fediverse/ActivityPub outbox.

Not wired to a live service -- no fediverse service exists elsewhere in the
estate (cmd/) yet. Callers get an explicit "unavailable" result instead of a
guessed connection.
"""


class FediverseBridgeUnavailable(RuntimeError):
    pass


def publish_activity(activity: dict) -> None:
    raise FediverseBridgeUnavailable(
        "fediverse_bridge has no fediverse service to publish to yet"
    )
