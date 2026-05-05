"""Path normalization, address parsing, and identity validation.

This module implements the rules in §2 of the SPEC:

- Paths under the user's home directory are normalized to a leading
  ``~/`` (or just ``~`` for the home directory itself).
- Paths outside the home directory remain absolute.
- Address grammar:

  * ``home:<normalized-path>``                — broadcast in that home
  * ``home:<normalized-path>#<sid-or-alias>`` — direct to one session
  * ``topic:<name>``                          — pub/sub topic

- Sessions IDs (``sid``): ``sess_`` + 16 url-safe base64 characters.
- Aliases: ``^[a-z][a-z0-9_-]{0,63}$`` and not starting with ``sess_``.
- Topic names: any UTF-8 with no leading ``/``, no ``#``, ≤ 256 bytes.

``blemees-peerd`` validates inputs against these rules at the wire boundary
and rejects malformed values with JSON-RPC error ``-32602``.
"""

from __future__ import annotations

import os
import re
import secrets
from dataclasses import dataclass
from typing import Literal

SID_PATTERN = re.compile(r"^sess_[A-Za-z0-9_-]{16}$")
ALIAS_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
TOPIC_MAX_BYTES = 256


# --- Path normalization -----------------------------------------------------


def normalize_path(path: str) -> str:
    """Return the canonical peer-identity form of *path*.

    Resolves symlinks via ``os.path.realpath`` and strips trailing
    separators, then rewrites paths under the user's home directory
    with a leading ``~`` (matching the SPEC §2 rule).
    """
    real = os.path.realpath(path)
    if real != "/" and real.endswith(os.sep):
        real = real.rstrip(os.sep)
    user_home = os.path.realpath(os.path.expanduser("~"))
    if real == user_home:
        return "~"
    home_prefix = user_home + os.sep
    if real.startswith(home_prefix):
        return "~/" + real[len(home_prefix):]
    return real


def is_normalized_path(path: str) -> bool:
    """Return True iff *path* is already in canonical form per §2.

    Used by ``blemees-peerd`` to validate ``peer.hello`` arguments without
    re-canonicalizing them — the harness/sidecar that connected is
    responsible for getting this right; ``blemees-peerd`` just checks the form.
    """
    if not path:
        return False
    if path == "~":
        return True
    if path.startswith("~/"):
        rest = path[2:]
        return _is_clean_relative(rest)
    if path.startswith("/"):
        return _is_clean_absolute(path)
    return False


def _is_clean_relative(rest: str) -> bool:
    if not rest or rest.startswith("/") or rest.endswith("/"):
        return False
    parts = rest.split("/")
    return all(p and p not in (".", "..") for p in parts)


def _is_clean_absolute(path: str) -> bool:
    if path == "/":
        return True
    if path.endswith("/"):
        return False
    parts = path.split("/")[1:]  # drop the leading empty
    return all(p and p not in (".", "..") for p in parts)


# --- Address parsing --------------------------------------------------------


@dataclass(frozen=True, slots=True)
class HomeAddress:
    home: str
    discriminator: str | None  # None = broadcast; otherwise sid or alias

    kind: Literal["home"] = "home"

    def render(self) -> str:
        if self.discriminator is None:
            return f"home:{self.home}"
        return f"home:{self.home}#{self.discriminator}"


@dataclass(frozen=True, slots=True)
class TopicAddress:
    name: str

    kind: Literal["topic"] = "topic"

    def render(self) -> str:
        return f"topic:{self.name}"


Address = HomeAddress | TopicAddress


class InvalidAddressError(ValueError):
    """Raised when an address string fails validation."""


def parse_address(addr: str) -> Address:
    """Parse and validate an address string.

    Raises ``InvalidAddressError`` if the input is not a well-formed
    address per §2.
    """
    if not isinstance(addr, str) or not addr:
        raise InvalidAddressError("address must be a non-empty string")

    if addr.startswith("topic:"):
        name = addr[len("topic:") :]
        if not _is_valid_topic_name(name):
            raise InvalidAddressError(f"invalid topic name: {name!r}")
        return TopicAddress(name=name)

    if addr.startswith("home:"):
        rest = addr[len("home:") :]
        if "#" in rest:
            home, disc = rest.split("#", 1)
        else:
            home, disc = rest, None
        if not is_normalized_path(home):
            raise InvalidAddressError(f"home path not normalized: {home!r}")
        if disc is not None and not _is_valid_discriminator(disc):
            raise InvalidAddressError(f"invalid discriminator: {disc!r}")
        return HomeAddress(home=home, discriminator=disc)

    raise InvalidAddressError(f"unknown address scheme: {addr!r}")


def _is_valid_topic_name(name: str) -> bool:
    if not name:
        return False
    if name.startswith("/") or "#" in name:
        return False
    return len(name.encode("utf-8")) <= TOPIC_MAX_BYTES


def _is_valid_discriminator(disc: str) -> bool:
    return is_valid_sid(disc) or is_valid_alias(disc)


# --- Identity tokens --------------------------------------------------------


def is_valid_sid(sid: str) -> bool:
    return bool(SID_PATTERN.match(sid))


def is_valid_alias(alias: str) -> bool:
    if not ALIAS_PATTERN.match(alias):
        return False
    return not alias.startswith("sess_")


def mint_sid() -> str:
    """Generate a fresh session ID per SPEC §3."""
    return f"sess_{secrets.token_urlsafe(12)}"
