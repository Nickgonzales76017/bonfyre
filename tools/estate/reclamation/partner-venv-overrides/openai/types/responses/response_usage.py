# File generated from our OpenAPI spec by Castiron. See CONTRIBUTING.md for details.

from ..._models import BaseModel

__all__ = ["ResponseUsage", "InputTokensDetails", "OutputTokensDetails"]


class InputTokensDetails(BaseModel):
    """A detailed breakdown of the input tokens."""

    cache_write_tokens: int = 0
    """The number of input tokens that were written to the cache.

    Bonfyre adapter normalization: providers that predate this field omit it.
    Absent means no cache writes were reported, which is 0. Defaulting here at
    the provider-response boundary keeps the field exact for callers instead of
    making it Optional and pushing ambiguity into every consumer.

    Without this, a completed and already-billed generation is discarded during
    usage accounting: the API returns {'cached_tokens': 0} with no
    cache_write_tokens, pydantic raises "Field required", and the operator marks
    the goal failed and retries it. Telemetry about a response must never
    invalidate the response.
    """

    cached_tokens: int
    """The number of tokens that were retrieved from the cache.

    [More on prompt caching](https://platform.openai.com/docs/guides/prompt-caching).
    """


class OutputTokensDetails(BaseModel):
    """A detailed breakdown of the output tokens."""

    reasoning_tokens: int
    """The number of reasoning tokens."""


class ResponseUsage(BaseModel):
    """
    Represents token usage details including input tokens, output tokens,
    a breakdown of output tokens, and the total tokens used.
    """

    input_tokens: int
    """The number of input tokens."""

    input_tokens_details: InputTokensDetails
    """A detailed breakdown of the input tokens."""

    output_tokens: int
    """The number of output tokens."""

    output_tokens_details: OutputTokensDetails
    """A detailed breakdown of the output tokens."""

    total_tokens: int
    """The total number of tokens used."""
