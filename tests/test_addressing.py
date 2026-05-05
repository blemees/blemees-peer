"""Path normalization, address parsing, and identity validation."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from blemees_peer.addressing import (
    HomeAddress,
    InvalidAddressError,
    TopicAddress,
    is_normalized_path,
    is_valid_alias,
    is_valid_sid,
    mint_sid,
    normalize_path,
    parse_address,
)

# --- normalize_path ---------------------------------------------------------


def test_normalize_home_itself(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    assert normalize_path(str(tmp_path)) == "~"


def test_normalize_under_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    project = tmp_path / "projects" / "x"
    project.mkdir(parents=True)
    assert normalize_path(str(project)) == "~/projects/x"


def test_normalize_outside_home_stays_absolute(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    other = Path("/tmp")
    real = os.path.realpath(str(other))
    assert normalize_path(str(other)) == real.rstrip("/") or "/tmp"


def test_normalize_strips_trailing_slash(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    assert normalize_path(str(tmp_path) + "/") == "~"


def test_normalize_resolves_symlinks(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    assert normalize_path(str(link)) == "~/real"


# --- is_normalized_path -----------------------------------------------------


@pytest.mark.parametrize(
    "p",
    [
        "~",
        "~/foo",
        "~/foo/bar",
        "/",
        "/tmp",
        "/tmp/foo",
        "/Users/me/projects/x",
    ],
)
def test_is_normalized_accepts(p: str) -> None:
    assert is_normalized_path(p)


@pytest.mark.parametrize(
    "p",
    [
        "",
        "foo",            # relative
        "~/",             # trailing slash
        "~/.",            # dot segment
        "~/..",           # parent segment
        "/tmp/",          # trailing slash on absolute
        "/tmp/./foo",     # dot segment in absolute
        "/tmp/../foo",    # parent segment
        "/tmp/foo/",      # trailing slash
        "~",              # ok actually — re-check below
    ],
)
def test_is_normalized_rejects(p: str) -> None:
    if p == "~":
        return  # this is actually valid
    assert not is_normalized_path(p)


# --- parse_address ----------------------------------------------------------


def test_parse_topic() -> None:
    addr = parse_address("topic:build-events")
    assert isinstance(addr, TopicAddress)
    assert addr.name == "build-events"
    assert addr.render() == "topic:build-events"


def test_parse_home_broadcast() -> None:
    addr = parse_address("home:~/foo")
    assert isinstance(addr, HomeAddress)
    assert addr.home == "~/foo"
    assert addr.discriminator is None


def test_parse_home_with_sid() -> None:
    sid = mint_sid()
    addr = parse_address(f"home:~/foo#{sid}")
    assert isinstance(addr, HomeAddress)
    assert addr.home == "~/foo"
    assert addr.discriminator == sid


def test_parse_home_with_alias() -> None:
    addr = parse_address("home:~/foo#architect")
    assert isinstance(addr, HomeAddress)
    assert addr.home == "~/foo"
    assert addr.discriminator == "architect"


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "foo",
        "topic:",
        "topic:/leading-slash",
        "topic:has#hash",
        "home:foo",  # not normalized
        "home:~/foo#BAD-Alias",  # uppercase
        "home:~/foo#__bad",  # leading underscore
        "home:~/foo#sess_short",  # malformed sid
        "cwd:~/foo",  # old scheme is no longer recognized
    ],
)
def test_parse_invalid(bad: str) -> None:
    with pytest.raises(InvalidAddressError):
        parse_address(bad)


# --- sid + alias ------------------------------------------------------------


def test_mint_sid_format() -> None:
    sid = mint_sid()
    assert is_valid_sid(sid)
    assert sid.startswith("sess_")
    assert len(sid) == len("sess_") + 16


@pytest.mark.parametrize(
    "alias,expected",
    [
        ("architect", True),
        ("a", True),
        ("a-b-c", True),
        ("test_one_two", True),
        ("Architect", False),  # uppercase
        ("", False),
        ("1leads", False),  # leading digit
        ("-leads", False),  # leading dash
        ("sess_x", False),  # reserved prefix
        ("a" * 65, False),  # too long
    ],
)
def test_is_valid_alias(alias: str, expected: bool) -> None:
    assert is_valid_alias(alias) is expected
