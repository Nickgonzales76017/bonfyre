"""Bridge to BonfyreAuth.

Not wired to a live endpoint yet -- cmd/BonfyreAuth exists in the estate but
this app doesn't have a confirmed local port/API contract for it. Callers
get an explicit "unavailable" result instead of a guessed connection.
"""


class AuthBridgeUnavailable(RuntimeError):
    pass


def verify_token(token: str) -> dict:
    raise AuthBridgeUnavailable(
        "auth_bridge is not wired to a running BonfyreAuth endpoint yet"
    )
