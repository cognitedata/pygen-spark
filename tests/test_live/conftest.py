"""Shared fixtures for cognite-cogsail live tests (skipped when credentials are missing)."""

from __future__ import annotations

import pytest
from cognite.client import CogniteClient

from tests.test_live.cogsail import CogsailSeed, credentials_toml_path, ensure_cogsail_seed, load_cogsail_client


@pytest.fixture(scope="session")
def live_client() -> CogniteClient:
    path = credentials_toml_path()
    if not path.is_file():
        pytest.skip(f"Live credentials TOML not found: {path}")
    client = load_cogsail_client(path)
    if client is None:
        pytest.skip("TOML missing client_id / client_secret")
    return client


@pytest.fixture(scope="session")
def seeded_cogsail(live_client: CogniteClient) -> CogsailSeed:
    return ensure_cogsail_seed(live_client)
