"""Tests for CDF audit header helpers."""

from __future__ import annotations

import json

from cognite.pygen_spark.audit import cdf_audit_http_template_context


def test_cdf_audit_http_template_context_shape() -> None:
    """Context JSON literals parse back; x-cdp-app follows pygen-main-style segments."""
    ctx = cdf_audit_http_template_context(
        audit_component="SessionScopedUDTF",
        audit_tail="Databricks",
        pygen_spark_version="9.9.9",
    )
    assert json.loads(ctx["cdf_ps_version_json"]) == "9.9.9"
    x_app = json.loads(ctx["cdf_x_cdp_app_json"])
    assert x_app == "CognitePygenSpark:9.9.9:SessionScopedUDTF:Databricks"


def test_cdf_audit_http_template_context_sanitizes_colons_in_segments() -> None:
    """Colons in version, component, or tail must not add extra x-cdp-app segments."""
    ctx = cdf_audit_http_template_context(
        audit_component="Comp:With:Colons",
        audit_tail="tail:extra",
        pygen_spark_version="1.0:rc1",
    )
    assert json.loads(ctx["cdf_ps_version_json"]) == "1.0:rc1"
    x_app = json.loads(ctx["cdf_x_cdp_app_json"])
    assert x_app == "CognitePygenSpark:1.0_rc1:Comp_With_Colons:tail_extra"
    assert x_app.count(":") == 3
