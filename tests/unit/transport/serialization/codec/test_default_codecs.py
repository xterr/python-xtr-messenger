from __future__ import annotations

from xtr_messenger import default_codecs


def test_default_codecs_prefer_pydantic_when_it_is_installed() -> None:
    """A producer and a consumer configured separately still agree: the consumer
    decodes what the producer happily encoded."""
    assert [type(codec).__name__ for codec in default_codecs()] == [
        "PydanticCodec",
        "DataclassCodec",
    ]


def test_default_codecs_always_handle_dataclasses() -> None:
    assert any(type(codec).__name__ == "DataclassCodec" for codec in default_codecs())
