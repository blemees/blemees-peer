"""The package version has one source: ``_version.py``."""

from __future__ import annotations

import importlib.metadata

import blemees_peer


def test_installed_metadata_matches_version_module() -> None:
    # pyproject.toml reads the version from _version.py (hatch dynamic
    # version), so the installed metadata must agree with __version__.
    assert importlib.metadata.version("blemees-peer") == blemees_peer.__version__
