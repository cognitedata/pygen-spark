"""Unit tests for dev.py release helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from dev import _get_change, _read_last_commit_message, create_changelog_entry

REPO_ROOT = Path(__file__).resolve().parents[2]
LAST_GIT_MESSAGE_FILE = REPO_ROOT / "last_git_message.txt"
CHANGELOG_ENTRY_FILE = REPO_ROOT / "last_changelog_entry.md"


@pytest.fixture(autouse=True)
def _restore_last_git_message_file() -> None:
    """Restore last_git_message.txt if a test overwrites it."""
    original = LAST_GIT_MESSAGE_FILE.read_text() if LAST_GIT_MESSAGE_FILE.exists() else None
    yield
    if original is None:
        LAST_GIT_MESSAGE_FILE.unlink(missing_ok=True)
    else:
        LAST_GIT_MESSAGE_FILE.write_text(original)
    CHANGELOG_ENTRY_FILE.unlink(missing_ok=True)


def test_read_last_commit_message_ignores_inline_bump_mentions() -> None:
    """Section headers must be line-anchored; inline ``## Bump`` must not split early."""
    LAST_GIT_MESSAGE_FILE.write_text(
        """release 0.3.2

Intro mentions `## Bump` / `## Changelog` inline in backticks.

## Bump

- [x] Patch
- [ ] Minor
- [ ] Major
- [ ] Skip

## Changelog

### Fixed

- Example fix
"""
    )
    bump_text, changelog_text = _read_last_commit_message()
    assert bump_text == "- [x] Patch\n- [ ] Minor\n- [ ] Major\n- [ ] Skip"
    assert changelog_text == "### Fixed\n\n- Example fix"
    assert _get_change(bump_text) == "patch"


def test_create_changelog_entry_from_release_message() -> None:
    """End-to-end changelog extraction for a valid release merge message."""
    LAST_GIT_MESSAGE_FILE.write_text(
        """Release 0.3.2

## Bump

- [x] Patch
- [ ] Minor
- [ ] Major
- [ ] Skip

## Changelog

### Fixed

- Memory fix

### Improved

- Faster parsing
"""
    )
    create_changelog_entry()
    assert CHANGELOG_ENTRY_FILE.read_text() == "### Fixed\n\n- Memory fix\n\n### Improved\n\n- Faster parsing"
