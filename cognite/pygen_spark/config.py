"""CDF connection configuration for generic Spark clusters.

This module provides a Pydantic model for CDF credentials that works with
any Spark cluster, not limited to Databricks.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, field_validator

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.credentials import OAuthClientCredentials


class CDFConnectionConfig(BaseModel):
    """CDF connection configuration aligned with pygen-main's load_cognite_client_from_toml.

    This model ensures consistent URL construction across all templates and UDTF implementations,
    matching the behavior of CogniteClient.default_oauth_client_credentials().

    The SDK expects cdf_cluster to be just the cluster name (e.g., "greenfield"), and automatically
    constructs:
    - Base URL: https://{cdf_cluster}.cognitedata.com
    - Scopes: [https://{cdf_cluster}.cognitedata.com/.default]
    - Token URL: https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token

    Args:
        client_id: OAuth2 client ID
        client_secret: OAuth2 client secret
        tenant_id: Azure AD tenant ID
        cdf_cluster: CDF cluster name (e.g., "greenfield", "westeurope-1") - just the name, not full domain
        project: CDF project name

    Examples:
        >>> config = CDFConnectionConfig(
        ...     client_id="...",
        ...     client_secret="...",
        ...     tenant_id="...",
        ...     cdf_cluster="greenfield",  # Just cluster name
        ...     project="my-project"
        ... )
        >>> config.base_url
        'https://greenfield.cognitedata.com'
        >>> config.scopes
        ['https://greenfield.cognitedata.com/.default']
    """

    client_id: str = Field(..., description="OAuth2 client ID")
    client_secret: str = Field(..., description="OAuth2 client secret")
    tenant_id: str = Field(..., description="Azure AD tenant ID")
    cdf_cluster: str = Field(..., description="CDF cluster name (e.g., 'greenfield')")
    project: str = Field(..., description="CDF project name")

    @field_validator("cdf_cluster")
    @classmethod
    def normalize_cluster(cls, v: str) -> str:
        """Normalize cluster name to ensure it's just the cluster name.

        Handles cases where full domain might be provided:
        - "greenfield" -> "greenfield"
        - "greenfield.cognitedata.com" -> "greenfield"
        - "https://greenfield.cognitedata.com" -> "greenfield"

        This ensures compatibility with both the SDK's expectations and
        cases where users might provide the full domain.
        """
        # Remove protocol if present
        if v.startswith("https://"):
            v = v[8:]
        elif v.startswith("http://"):
            v = v[7:]

        # Remove domain suffix if present
        if v.endswith(".cognitedata.com"):
            v = v[:-16]  # Remove ".cognitedata.com"

        return v.strip()

    @property
    def base_url(self) -> str:
        """Get the CDF base URL (aligned with CogniteClient.default).

        Returns:
            Base URL in format: https://{cluster}.cognitedata.com
        """
        return f"https://{self.cdf_cluster}.cognitedata.com"

    @property
    def scopes(self) -> list[str]:
        """Get OAuth2 scopes (aligned with OAuthClientCredentials.default_for_azure_ad).

        Returns:
            List containing: https://{cluster}.cognitedata.com/.default
        """
        return [f"{self.base_url}/.default"]

    @property
    def token_url(self) -> str:
        """Get the OAuth2 token URL (aligned with OAuthClientCredentials.default_for_azure_ad).

        Returns:
            Token URL: https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
        """
        return f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"

    def create_credentials(self) -> OAuthClientCredentials:
        """Create OAuth2 credentials using SDK's default_for_azure_ad (aligned with pygen-main).

        This uses the SDK's built-in method to ensure consistency with
        CogniteClient.default_oauth_client_credentials().

        Returns:
            OAuthClientCredentials instance

        Raises:
            ImportError: If cognite-sdk is not installed
        """
        from cognite.client.credentials import OAuthClientCredentials

        return OAuthClientCredentials.default_for_azure_ad(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            cdf_cluster=self.cdf_cluster,
        )

    def create_client(self, client_name: str = "spark-udtf", **kwargs: dict[str, object]) -> CogniteClient:
        """Create a CogniteClient using SDK's default_oauth_client_credentials (aligned with pygen-main).

        This method uses the same approach as load_cognite_client_from_toml() in pygen-main,
        ensuring consistency across the ecosystem.

        Args:
            client_name: Name for the client (default: "spark-udtf")
            **kwargs: Additional arguments to pass to CogniteClient

        Returns:
            CogniteClient instance configured with this connection

        Raises:
            ImportError: If cognite-sdk is not installed
        """
        from cognite.client import CogniteClient

        # Use SDK's default method (same as load_cognite_client_from_toml)
        return CogniteClient.default_oauth_client_credentials(
            project=self.project,
            cdf_cluster=self.cdf_cluster,
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            client_name=client_name,
            **kwargs,
        )

    @classmethod
    def from_toml(cls, toml_file: str | Path = "config.toml", section: str | None = "cognite") -> CDFConnectionConfig:
        """Load configuration from a TOML file (aligned with load_cognite_client_from_toml).

        Args:
            toml_file: Path to TOML file
            section: Section name in TOML file (default: "cognite")

        Returns:
            CDFConnectionConfig instance

        Examples:
            >>> config = CDFConnectionConfig.from_toml("config.toml")  # doctest: +SKIP
            >>> client = config.create_client()  # doctest: +SKIP
        """
        toml_path = Path(toml_file)
        if not toml_path.exists():
            raise FileNotFoundError(f"TOML file not found: {toml_file}")

        # Try tomli first (Python < 3.11), then tomllib (Python 3.11+)
        try:
            import tomli

            with toml_path.open("rb") as f:
                toml_content = tomli.load(f)
        except ImportError:
            try:
                import tomllib  # type: ignore[import-not-found]

                with toml_path.open("rb") as f:
                    toml_content = tomllib.load(f)
            except ImportError:
                raise ImportError(
                    "TOML library required. Install with: pip install tomli or use Python 3.11+"
                ) from None

        if section is not None:
            toml_content = toml_content[section]

        return cls(**toml_content)
