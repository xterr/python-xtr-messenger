from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from xtr_http_kernel.data_collector import clone_var

from xtr_messenger.data_collector._masking import MASK, is_sensitive, masked


@dataclass(frozen=True, slots=True)
class _Login:
    user: str
    password: str


def test_a_credential_named_attribute_keeps_its_name_and_loses_its_value() -> None:
    tree = masked(clone_var(_Login(user="ada", password="hunter2")))  # noqa: S106 — a fixture value

    attributes = dict(cast("list[tuple[str, dict[str, object]]]", tree["attributes"]))
    assert attributes["password"] == clone_var(MASK)
    assert attributes["user"] == clone_var("ada")


def test_a_credential_header_in_a_mapping_is_masked() -> None:
    tree = masked(clone_var({"Authorization": "Bearer abc", "accept": "text/plain"}))

    assert tree["items"] == [
        [clone_var("Authorization"), clone_var(MASK)],
        [clone_var("accept"), clone_var("text/plain")],
    ]


def test_a_credential_inside_a_sequence_is_masked() -> None:
    tree = masked(clone_var([{"api_key": "k-1"}]))

    [inner] = cast("list[dict[str, object]]", tree["items"])
    assert inner["items"] == [[clone_var("api_key"), clone_var(MASK)]]


def test_a_mapping_with_keys_that_are_not_text_is_left_alone() -> None:
    tree = clone_var({1: "secret value"})

    assert masked(tree) == tree


def test_a_scalar_is_returned_as_it_is() -> None:
    tree = clone_var("plain")

    assert masked(tree) is tree


def test_names_are_told_sensitive_by_header_and_by_field_word() -> None:
    names = ("Set-Cookie", "db_PASSWORD", "refresh_token", "name")

    assert [is_sensitive(name) for name in names] == [True, True, True, False]
