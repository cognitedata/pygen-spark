"""CDF audit header helpers for generated UDTF HTTP clients.

Mirrors pygen-main's ``CognitePygen:{version}:{Component}:{tail}`` pattern for ``client_name`` /
``x-cdp-app``, using ``CognitePygenSpark`` as the suite name and ``cognite-pygen-spark``'s version.
"""

from __future__ import annotations

import json

from cognite.pygen_spark._version import __version__ as PYGEN_SPARK_PACKAGE_VERSION


def cdf_audit_http_template_context(
    *,
    audit_component: str,
    audit_tail: str,
    pygen_spark_version: str | None = None,
) -> dict[str, str]:
    """Build Jinja context for embedding CDF audit strings into generated UDTF code.

    Args:
        audit_component: Third segment, e.g. ``SessionScopedUDTF``, ``UnityCatalogUDTF``,
            ``StandaloneUDTF``.
        audit_tail: Fourth segment, e.g. ``Databricks``, ``GenericSpark``.
        pygen_spark_version: Override package version; defaults to ``cognite.pygen_spark.__version__``.

    Returns:
        Keys ``cdf_ps_version_json`` and ``cdf_x_cdp_app_json``: JSON-encoded Python literals
        (safe to emit as ``key = {{ value }}`` in templates). Colons in version, component, or
        tail are replaced with underscores in ``x-cdp-app`` so CDF sees exactly four segments.
    """
    ver = pygen_spark_version if pygen_spark_version is not None else PYGEN_SPARK_PACKAGE_VERSION
    s_ver, s_comp, s_tail = (s.replace(":", "_") for s in (ver, audit_component, audit_tail))
    x_cdp_app = f"CognitePygenSpark:{s_ver}:{s_comp}:{s_tail}"
    return {
        "cdf_ps_version_json": json.dumps(ver),
        "cdf_x_cdp_app_json": json.dumps(x_cdp_app),
    }
