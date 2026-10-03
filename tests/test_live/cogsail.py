"""cognite-cogsail live test setup: client loading and instance-space seed data.

The sailboat model keeps its instances in the model space ``sailboat``, which hides bugs where
instance space and view space are confused. The seed writes ``SmallBoat`` nodes into dedicated
instance spaces so ``instance_space`` pushdown is exercised against data that differs from the view space.

Run the seed standalone (idempotent)::

    uv run python -m tests.test_live.cogsail

Credentials come from ``CDF_CREDENTIALS_TOML`` (default below). Secrets are never printed.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials, Token
from pydantic import BaseModel, Field

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib  # type: ignore[import-not-found,no-redef]

DEFAULT_TOML = Path(r"C:\Users\FredrikHolm\Downloads\PRD-Data Quality\cog-sail-client_id.toml")


class SeedBoat(BaseModel):
    """A SmallBoat node written to a seed instance space."""

    external_id: str
    name: str
    description: str | None = None


class SeedCertificate(BaseModel):
    """An ORCCertificate node; ``aph_tod`` is numeric, so MIN / MAX can be pushed to CDF."""

    external_id: str
    name: str
    aph_tod: float


class SeedSpace(BaseModel):
    """An instance space and the SmallBoat / ORCCertificate nodes it should contain."""

    space: str
    name: str
    boats: list[SeedBoat] = Field(default_factory=list)
    certificates: list[SeedCertificate] = Field(default_factory=list)

    @property
    def external_ids(self) -> set[str]:
        """External IDs of the seeded boats."""
        return {boat.external_id for boat in self.boats}

    @property
    def described_external_ids(self) -> set[str]:
        """External IDs of seeded boats that have a description (``IS NOT NULL``)."""
        return {boat.external_id for boat in self.boats if boat.description is not None}


class CogsailSeed(BaseModel):
    """Seed data for the cogsail instance-space test setup."""

    view_space: str = "sailboat"
    view_external_id: str = "SmallBoat"
    view_version: str = "v1"
    certificate_view_external_id: str = "ORCCertificate"
    spaces: list[SeedSpace] = Field(default_factory=list)

    @property
    def view(self) -> dm.ViewId:
        """View the seed boats are written through."""
        return dm.ViewId(space=self.view_space, external_id=self.view_external_id, version=self.view_version)

    @property
    def certificate_view(self) -> dm.ViewId:
        """View the seed certificates are written through."""
        return dm.ViewId(
            space=self.view_space, external_id=self.certificate_view_external_id, version=self.view_version
        )

    @property
    def by_space(self) -> dict[str, SeedSpace]:
        """Convenience property for dict-like access."""
        return {space.space: space for space in self.spaces}


cogsail_seed = CogsailSeed(
    spaces=[
        SeedSpace(
            space="inst_sailboat_fleet_a",
            name="Sailboat fleet A instance space",
            boats=[
                SeedBoat(
                    external_id="seed_small_boat_fleet_a_01",
                    name="Seed Fleet A 01",
                    description="Pushdown seed boat, fleet A",
                ),
                SeedBoat(
                    external_id="seed_small_boat_fleet_a_02",
                    name="Seed Fleet A 02",
                    description="Pushdown seed boat, fleet A",
                ),
                SeedBoat(external_id="seed_small_boat_fleet_a_03", name="Seed Fleet A 03", description=None),
            ],
            certificates=[
                SeedCertificate(external_id="seed_orc_certificate_fleet_a_01", name="Seed ORC A 01", aph_tod=480.5),
                SeedCertificate(external_id="seed_orc_certificate_fleet_a_02", name="Seed ORC A 02", aph_tod=512.0),
                SeedCertificate(external_id="seed_orc_certificate_fleet_a_03", name="Seed ORC A 03", aph_tod=530.25),
            ],
        ),
        SeedSpace(
            space="inst_sailboat_fleet_b",
            name="Sailboat fleet B instance space",
            boats=[
                SeedBoat(
                    external_id="seed_small_boat_fleet_b_01",
                    name="Seed Fleet B 01",
                    description="Pushdown seed boat, fleet B",
                ),
                SeedBoat(
                    external_id="seed_small_boat_fleet_b_02",
                    name="Seed Fleet B 02",
                    description="Pushdown seed boat, fleet B",
                ),
            ],
            certificates=[
                SeedCertificate(external_id="seed_orc_certificate_fleet_b_01", name="Seed ORC B 01", aph_tod=600.0),
            ],
        ),
    ]
)


def credentials_toml_path() -> Path:
    """Path to the cogsail credentials TOML (``CDF_CREDENTIALS_TOML`` overrides the default)."""
    override = os.environ.get("CDF_CREDENTIALS_TOML")
    return Path(override) if override else DEFAULT_TOML


def load_cogsail_client(path: Path) -> CogniteClient | None:
    """Build a CogniteClient from the cogsail TOML, or None when credentials are incomplete."""
    with path.open("rb") as fh:
        data: dict[str, Any] = tomllib.load(fh)

    cognite = data["cognite"]
    runtime = data.get("fn_btp_hierarchy_runtime") or {}
    client_id = runtime.get("client_id") or data.get("hierarchy_workflow_triggers", {}).get("ingest_trigger_client_id")
    client_secret = runtime.get("client_secret") or data.get("hierarchy_workflow_trigger_secrets", {}).get(
        "ingest_trigger_client_secret"
    )
    cluster = cognite.get("cdf_cluster")
    project = cognite.get("project")
    if not cluster or not project:
        return None
    base_url = f"https://{cluster}.cognitedata.com"

    if client_id and client_secret:
        tenant_id = cognite.get("idp_tenant_id") or cognite.get("tenant_id")
        if not tenant_id:
            return None
        credentials = OAuthClientCredentials(
            token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=[f"{base_url}/.default"],
        )
    else:
        bearer_token = cognite.get("bearer_token")
        if not isinstance(bearer_token, str) or not bearer_token:
            return None
        token_value = bearer_token
        credentials = Token(lambda: token_value)

    return CogniteClient(
        ClientConfig(
            client_name="pygen-spark-live-tests",
            project=str(project),
            credentials=credentials,
            base_url=base_url,
        )
    )


def ensure_cogsail_seed(client: CogniteClient, seed: CogsailSeed = cogsail_seed) -> CogsailSeed:
    """Create the seed instance spaces and upsert their SmallBoat nodes (idempotent).

    ``boat_guid`` / ``small_boat_guid`` are written so the nodes satisfy the view's ``hasData`` filter;
    without them ``instances/list`` returns the nodes but ``instances/aggregate`` does not count them.
    """
    client.data_modeling.spaces.apply([dm.SpaceApply(space=space.space, name=space.name) for space in seed.spaces])
    nodes = [
        dm.NodeApply(
            space=space.space,
            external_id=boat.external_id,
            sources=[
                dm.NodeOrEdgeData(
                    source=seed.view,
                    properties={
                        "name": boat.name,
                        "description": boat.description,
                        "boat_guid": boat.external_id,
                        "small_boat_guid": boat.external_id,
                    },
                )
            ],
        )
        for space in seed.spaces
        for boat in space.boats
    ]
    # orc_certificate_guid / aph_tod live in the ORCCertificate container the view's hasData filter requires
    nodes += [
        dm.NodeApply(
            space=space.space,
            external_id=certificate.external_id,
            sources=[
                dm.NodeOrEdgeData(
                    source=seed.certificate_view,
                    properties={
                        "name": certificate.name,
                        "aph_tod": certificate.aph_tod,
                        "orc_certificate_guid": certificate.external_id,
                    },
                )
            ],
        )
        for space in seed.spaces
        for certificate in space.certificates
    ]
    client.data_modeling.instances.apply(nodes=nodes)
    return seed


def main() -> None:
    """Seed cogsail from the command line."""
    path = credentials_toml_path()
    if not path.is_file():
        raise SystemExit(f"Credentials TOML not found: {path}")
    client = load_cogsail_client(path)
    if client is None:
        raise SystemExit(f"Credentials TOML is missing client_id / client_secret: {path}")

    seed = ensure_cogsail_seed(client)
    for space in seed.spaces:
        print(
            f"{space.space}: {len(space.boats)} SmallBoat, {len(space.certificates)} ORCCertificate nodes"
            f" ({sorted(space.external_ids)})"
        )


if __name__ == "__main__":
    main()
