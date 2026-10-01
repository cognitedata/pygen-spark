"""Synthetic LIMS result view and 100,000-node seed for cognite-cogsail.

Space layout (schema space is not an instance space):

- ``dm_dom_lims_result`` holds container ``LimsResult``, view ``LimsResult`` / ``v1``,
  and data model ``LimsResult_DOM`` / ``v1``.
- ``inst_lims_result_lab_a`` holds ``result_000001`` .. ``result_060000`` (60,000 nodes).
- ``inst_lims_result_lab_b`` holds ``result_060001`` .. ``result_100000`` (40,000 nodes).

The column set follows the 40-column LIMS result entity: camelCase CDF properties, CDF primitives.
Nodes are generated from ``result_id`` (no checked-in dump). ``result_000001`` is the equality fixture.

Run the seed standalone (idempotent)::

    uv run python -m tests.test_live.lims_result

Credentials come from ``CDF_CREDENTIALS_TOML``. Secrets are never printed.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Literal

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.containers import BTreeIndex, Index
from cognite.client.data_classes.data_modeling.views import ViewPropertyApply
from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel, ConfigDict, Field, create_model

from cognite.pygen_spark.filters import (
    AggregateMetric,
    AggregateRequestSpec,
    FilterSpec,
    build_aggregate_payload,
    build_filter_json,
    parse_aggregate_count,
)
from tests.test_live.cogsail import credentials_toml_path, load_cogsail_client

CdfTypeName = Literal["text", "int32", "int64", "float64", "boolean", "timestamp"]

DM_SPACE = "dm_dom_lims_result"
LAB_A = "inst_lims_result_lab_a"
LAB_B = "inst_lims_result_lab_b"
VIEW_EXTERNAL_ID = "LimsResult"
VIEW_VERSION = "v1"
MODEL_EXTERNAL_ID = "LimsResult_DOM"
CONTAINER_EXTERNAL_ID = "LimsResult"

INSTANCE_COUNT = 100_000
LAB_A_COUNT = 60_000
LAB_B_COUNT = 40_000
FIXTURE_ID = 1
FIXTURE_EXTERNAL_ID = "result_000001"
FIXTURE_NUMERIC_VALUE = 12.5
MOISTURE_CONTENT = "MOISTURE_CONTENT"
MOISTURE_CONTENT_COUNT = 10_000
OUT_OF_SPEC_COUNT = 1_000
OUT_OF_SPEC_NUMERIC_VALUE = 1500.0
ALIQUOT_ABSENT_COUNT = 20_000
LONG_COMMENT_LENGTH = 3_500
APPLY_BATCH_SIZE = 200

ENTERED_EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)
JANUARY_START = ENTERED_EPOCH
FEBRUARY_START = datetime(2024, 2, 1, tzinfo=timezone.utc)
MIN_SPEC_LIMIT = 0.0
MAX_SPEC_LIMIT = 1000.0

_COMPONENTS = (
    "RELATED_SUBSTANCE_A",
    MOISTURE_CONTENT,
    "ENDOTOXIN_CONC",
    "POTENCY",
    "PH",
    "APPEARANCE",
    "DENSITY",
    "VISCOSITY",
    "ASSAY",
    "IMPURITY_TOTAL",
)
RESULT_TYPES = ("NUMERIC", "TEXT", "LIST", "CALCULATED", "FILE")
_STATUS_CODES = ("UNENTERED", "ENTERED", "CALCULATED", "REVIEWED", "AUTHORIZED", "REJECTED")
_INDEXED = (
    "resultId",
    "sampleId",
    "componentName",
    "numericValue",
    "isOutOfSpec",
    "enteredTimestamp",
    "statusCode",
)


class ColumnSpec(BaseModel):
    """One LIMS result column mapped onto a CDF container property."""

    name: str
    cdf_type: CdfTypeName
    required: bool
    description: str


COLUMNS: tuple[ColumnSpec, ...] = (
    ColumnSpec(
        name="resultId", cdf_type="int64", required=True, description="Surrogate primary key of the analytical result."
    ),
    ColumnSpec(
        name="testId",
        cdf_type="int64",
        required=True,
        description="Parent test execution identifier.",
    ),
    ColumnSpec(
        name="sampleId",
        cdf_type="int64",
        required=True,
        description="Denormalized parent sample identifier.",
    ),
    ColumnSpec(
        name="aliquotId",
        cdf_type="text",
        required=False,
        description="Specimen subdivision identifier.",
    ),
    ColumnSpec(
        name="batchOrWorklistId",
        cdf_type="text",
        required=False,
        description="Analytical run, worklist, or preparation batch.",
    ),
    ColumnSpec(
        name="componentName", cdf_type="text", required=True, description="Canonical analyte or parameter name."
    ),
    ColumnSpec(
        name="resultType", cdf_type="text", required=True, description="NUMERIC, TEXT, LIST, CALCULATED, or FILE."
    ),
    ColumnSpec(
        name="rawValue",
        cdf_type="text",
        required=False,
        description="Unaltered value as received from the instrument or analyst.",
    ),
    ColumnSpec(
        name="formattedEntry",
        cdf_type="text",
        required=False,
        description="Human-readable value used on a certificate of analysis.",
    ),
    ColumnSpec(
        name="numericValue",
        cdf_type="float64",
        required=False,
        description="Floating-point measurement used for calculation and charting.",
    ),
    ColumnSpec(
        name="roundedValue", cdf_type="float64", required=False, description="Measurement after method rounding rules."
    ),
    ColumnSpec(
        name="reportingUnits",
        cdf_type="text",
        required=False,
        description="Controlled unit of measure.",
    ),
    ColumnSpec(
        name="significantFigures",
        cdf_type="int32",
        required=False,
        description="Decimal places required by the method.",
    ),
    ColumnSpec(
        name="specRuleId",
        cdf_type="text",
        required=False,
        description="Specification profile and revision applied at assignment.",
    ),
    ColumnSpec(
        name="minSpecLimit", cdf_type="float64", required=False, description="Lower specification limit snapshot."
    ),
    ColumnSpec(
        name="maxSpecLimit", cdf_type="float64", required=False, description="Upper specification limit snapshot."
    ),
    ColumnSpec(
        name="minActionLimit",
        cdf_type="float64",
        required=False,
        description="Lower action limit snapshot.",
    ),
    ColumnSpec(
        name="maxActionLimit",
        cdf_type="float64",
        required=False,
        description="Upper action limit snapshot.",
    ),
    ColumnSpec(
        name="limitOfDetection", cdf_type="float64", required=False, description="Method detection limit snapshot."
    ),
    ColumnSpec(
        name="limitOfQuant", cdf_type="float64", required=False, description="Method quantification limit snapshot."
    ),
    ColumnSpec(
        name="qualificationFlag",
        cdf_type="text",
        required=False,
        description="PASS, FAIL, OOS, OOT, or WARN.",
    ),
    ColumnSpec(
        name="instrumentId", cdf_type="text", required=False, description="Identifier of the measuring instrument."
    ),
    ColumnSpec(
        name="analyticalMethodId",
        cdf_type="text",
        required=True,
        description="Standard operating procedure identifier and version.",
    ),
    ColumnSpec(
        name="reagentLotNumber",
        cdf_type="text",
        required=False,
        description="Consumed reagent, standard, or column lot.",
    ),
    ColumnSpec(
        name="calculationFormulaId",
        cdf_type="text",
        required=False,
        description="Calculation routine applied to the raw value.",
    ),
    ColumnSpec(
        name="dilutionFactor",
        cdf_type="float64",
        required=True,
        description="Multiplier back to the neat-matrix concentration.",
    ),
    ColumnSpec(
        name="runNumber", cdf_type="int32", required=False, description="Injection or replicate index within the batch."
    ),
    ColumnSpec(
        name="rawDataFilePath", cdf_type="text", required=False, description="Pointer to the raw instrument file."
    ),
    ColumnSpec(
        name="statusCode",
        cdf_type="text",
        required=True,
        description="UNENTERED, ENTERED, CALCULATED, REVIEWED, AUTHORIZED, or REJECTED.",
    ),
    ColumnSpec(
        name="isReportable",
        cdf_type="boolean",
        required=True,
        description="Whether the component is published on a certificate of analysis.",
    ),
    ColumnSpec(
        name="isOutOfSpec",
        cdf_type="boolean",
        required=True,
        description="Whether the measurement violates the specification snapshot.",
    ),
    ColumnSpec(
        name="isOutOfTrend",
        cdf_type="boolean",
        required=True,
        description="Whether the measurement is an atypical statistical drift.",
    ),
    ColumnSpec(
        name="retestFlag",
        cdf_type="boolean",
        required=True,
        description="Whether the result comes from an authorized retest.",
    ),
    ColumnSpec(
        name="enteredByUserId",
        cdf_type="text",
        required=False,
        description="Account that posted the result.",
    ),
    ColumnSpec(
        name="enteredTimestamp",
        cdf_type="timestamp",
        required=False,
        description="UTC time of initial result entry.",
    ),
    ColumnSpec(
        name="reviewedByUserId", cdf_type="text", required=False, description="Account that peer-reviewed the result."
    ),
    ColumnSpec(
        name="reviewedTimestamp",
        cdf_type="timestamp",
        required=False,
        description="UTC time of peer review.",
    ),
    ColumnSpec(
        name="changeReasonCode",
        cdf_type="text",
        required=False,
        description="Controlled reason code for a change to a saved result.",
    ),
    ColumnSpec(
        name="auditComment",
        cdf_type="text",
        required=False,
        description="Narrative justification for a change to a saved result.",
    ),
    ColumnSpec(
        name="rowVersionChecksum",
        cdf_type="text",
        required=True,
        description="Row checksum used as a concurrency and tamper token.",
    ),
)


def _field_type(spec: ColumnSpec) -> object:
    py_type: type = {"text": str, "int32": int, "int64": int, "float64": float, "boolean": bool, "timestamp": str}[
        spec.cdf_type
    ]
    if spec.required:
        return (py_type, Field(description=spec.description))
    return (py_type | None, Field(default=None, description=spec.description))


LimsResultProperties: type[BaseModel] = create_model(
    "LimsResultProperties",
    __config__=ConfigDict(extra="forbid"),
    **{spec.name: _field_type(spec) for spec in COLUMNS},  # type: ignore[call-overload]
)


def _cdf_property_type(cdf_type: CdfTypeName) -> dm.PropertyType:
    if cdf_type == "text":
        return dm.Text()
    if cdf_type == "int32":
        return dm.Int32()
    if cdf_type == "int64":
        return dm.Int64()
    if cdf_type == "float64":
        return dm.Float64()
    if cdf_type == "boolean":
        return dm.Boolean()
    return dm.Timestamp()


def external_id_for(result_id: int) -> str:
    """Node external id for a 1-based result id."""
    return f"result_{result_id:06d}"


def instance_space_for(result_id: int) -> str:
    """Instance space for a 1-based result id."""
    if result_id <= LAB_A_COUNT:
        return LAB_A
    return LAB_B


def result_type_for(result_id: int) -> str:
    """Structural result type. 70/15/8/5/2 percent NUMERIC/TEXT/LIST/CALCULATED/FILE."""
    bucket = result_id % 100
    if bucket < 70:
        return "NUMERIC"
    if bucket < 85:
        return "TEXT"
    if bucket < 93:
        return "LIST"
    if bucket < 98:
        kind = "CALCULATED"
    else:
        kind = "FILE"
    if kind not in RESULT_TYPES:
        raise ValueError(f"Unexpected result type: {kind}")
    return kind


def is_out_of_spec(result_id: int) -> bool:
    """Exactly 1,000 rows (every 100th id, excluding the fixture)."""
    return result_id != FIXTURE_ID and result_id % 100 == 0


def numeric_value_for(result_id: int) -> float | None:
    """Measurement. Null for TEXT, LIST, and FILE. Out-of-spec numeric rows are above the max spec."""
    if result_id == FIXTURE_ID:
        return FIXTURE_NUMERIC_VALUE
    if result_type_for(result_id) not in {"NUMERIC", "CALCULATED"}:
        return None
    if is_out_of_spec(result_id):
        return OUT_OF_SPEC_NUMERIC_VALUE
    return float(result_id % 1000)


def status_code_for(result_id: int) -> str:
    """Lifecycle status. The fixture row is AUTHORIZED."""
    if result_id == FIXTURE_ID:
        return "AUTHORIZED"
    return _STATUS_CODES[result_id % len(_STATUS_CODES)]


def entered_timestamp(result_id: int) -> datetime:
    """UTC entry time spread across 2024 by ``result_id % 366`` days from 1 January."""
    return ENTERED_EPOCH + timedelta(days=result_id % 366)


def timestamp_text(moment: datetime) -> str:
    """CDF timestamp literal (``YYYY-MM-DDTHH:MM:SS.mmmZ``)."""
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def properties_for(result_id: int) -> BaseModel:
    """Build the view properties for one result id."""
    numeric = numeric_value_for(result_id)
    kind = result_type_for(result_id)
    reviewed = status_code_for(result_id) in {"REVIEWED", "AUTHORIZED"}
    if numeric is not None:
        raw = f"{numeric:.1f}" if result_id == FIXTURE_ID else str(numeric)
    elif kind == "TEXT":
        raw = "Not Detected"
    elif kind == "LIST":
        raw = "Pass"
    else:
        raw = "chromatogram.raw"
    aliquot = None if result_id % 5 == 0 else f"ALQ-{result_id:06d}"
    comment: str | None
    if result_id == FIXTURE_ID:
        comment = "Fixture row for equality checks."
    elif result_id % 1000 == 0:
        comment = "A" * LONG_COMMENT_LENGTH
    elif result_id % 7 == 0:
        comment = None
    else:
        comment = f"Entered for result {result_id}."
    change_reason: str | None
    if result_id == FIXTURE_ID:
        change_reason = "INITIAL_ENTRY"
    elif result_id % 25 == 0:
        change_reason = "RETEST"
    else:
        change_reason = None
    if result_id == FIXTURE_ID:
        qualification = "PASS"
    elif is_out_of_spec(result_id):
        qualification = "OOS"
    else:
        qualification = ("PASS", "WARN", "OOT")[result_id % 3]
    reagent = None if result_id != FIXTURE_ID and result_id % 9 == 0 else f"LOT-{result_id % 50:04d}"
    formula = f"CALC-{result_id % 15:02d}" if result_id == FIXTURE_ID or kind == "CALCULATED" else None
    run_number = result_id % 96 if result_id == FIXTURE_ID or result_id % 11 != 0 else None
    raw_path = None if result_id != FIXTURE_ID and result_id % 3 == 0 else f"s3://lims/{external_id_for(result_id)}.raw"
    values: dict[str, str | int | float | bool | None] = {
        "resultId": result_id,
        "testId": (result_id - 1) // 4 + 1,
        "sampleId": (result_id - 1) // 20 + 1,
        "aliquotId": aliquot,
        "batchOrWorklistId": None if result_id % 17 == 0 else f"WL-{result_id % 200:03d}",
        "componentName": _COMPONENTS[result_id % 10],
        "resultType": kind,
        "rawValue": raw,
        "formattedEntry": raw,
        "numericValue": numeric,
        "roundedValue": numeric,
        "reportingUnits": "mg/L" if numeric is not None else None,
        "significantFigures": 3 if numeric is not None else None,
        "specRuleId": f"SPEC-{result_id % 6:02d}",
        "minSpecLimit": MIN_SPEC_LIMIT,
        "maxSpecLimit": MAX_SPEC_LIMIT,
        "minActionLimit": 10.0,
        "maxActionLimit": 900.0,
        "limitOfDetection": 0.05,
        "limitOfQuant": 0.2,
        "qualificationFlag": qualification,
        "instrumentId": f"HPLC-{result_id % 8:02d}",
        "analyticalMethodId": f"SOP-QC-{result_id % 12:02d}",
        "reagentLotNumber": reagent,
        "calculationFormulaId": formula,
        "dilutionFactor": 1.0 if result_id == FIXTURE_ID else float(1 + (result_id % 5)),
        "runNumber": run_number,
        "rawDataFilePath": raw_path,
        "statusCode": status_code_for(result_id),
        "isReportable": result_id % 4 != 0,
        "isOutOfSpec": is_out_of_spec(result_id),
        "isOutOfTrend": result_id != FIXTURE_ID and result_id % 50 == 0,
        "retestFlag": result_id != FIXTURE_ID and result_id % 25 == 0,
        "enteredByUserId": f"analyst_{result_id % 20:02d}",
        "enteredTimestamp": timestamp_text(entered_timestamp(result_id)),
        "reviewedByUserId": f"reviewer_{result_id % 10:02d}" if reviewed else None,
        "reviewedTimestamp": timestamp_text(entered_timestamp(result_id) + timedelta(days=1)) if reviewed else None,
        "changeReasonCode": change_reason,
        "auditComment": comment,
        "rowVersionChecksum": hashlib.sha256(external_id_for(result_id).encode()).hexdigest(),
    }
    return LimsResultProperties.model_validate(values)


@lru_cache(maxsize=2)
def numeric_bounds(space: str) -> tuple[float, float]:
    """Min and max ``numericValue`` stored in one instance space."""
    if space == LAB_A:
        ids = range(1, LAB_A_COUNT + 1)
    elif space == LAB_B:
        ids = range(LAB_A_COUNT + 1, INSTANCE_COUNT + 1)
    else:
        raise ValueError(f"Unknown instance space: {space}")
    values = [value for result_id in ids if (value := numeric_value_for(result_id)) is not None]
    return min(values), max(values)


@lru_cache(maxsize=1)
def january_entered_count() -> int:
    """Rows whose entry timestamp falls in January 2024."""
    return sum(
        1
        for result_id in range(1, INSTANCE_COUNT + 1)
        if JANUARY_START <= entered_timestamp(result_id) < FEBRUARY_START
    )


class LimsResultSeed(BaseModel):
    """Names and expected counts for the LIMS result seed. Instances are generated, not stored here."""

    view_space: str = DM_SPACE
    view_external_id: str = VIEW_EXTERNAL_ID
    view_version: str = VIEW_VERSION
    model_external_id: str = MODEL_EXTERNAL_ID
    container_external_id: str = CONTAINER_EXTERNAL_ID
    lab_a: str = LAB_A
    lab_b: str = LAB_B

    @property
    def view(self) -> dm.ViewId:
        """View the seed nodes are written through."""
        return dm.ViewId(space=self.view_space, external_id=self.view_external_id, version=self.view_version)

    @property
    def model(self) -> dm.DataModelId:
        """Data model that exposes the LIMS result view."""
        return dm.DataModelId(space=self.view_space, external_id=self.model_external_id, version=self.view_version)


lims_result_seed = LimsResultSeed()


def _container_apply(seed: LimsResultSeed) -> dm.ContainerApply:
    properties = {
        spec.name: dm.ContainerProperty(
            type=_cdf_property_type(spec.cdf_type),
            nullable=not spec.required,
            description=spec.description,
        )
        for spec in COLUMNS
    }
    indexes: dict[str, Index] = {name: BTreeIndex(properties=[name]) for name in _INDEXED}
    return dm.ContainerApply(
        space=seed.view_space,
        external_id=seed.container_external_id,
        name="LIMS result",
        description="Synthetic 40-column LIMS result container for pushdown tests.",
        used_for="node",
        properties=properties,
        indexes=indexes,
    )


def _view_apply(seed: LimsResultSeed) -> dm.ViewApply:
    container = dm.ContainerId(space=seed.view_space, external_id=seed.container_external_id)
    properties: dict[str, ViewPropertyApply] = {
        spec.name: dm.MappedPropertyApply(
            container=container,
            container_property_identifier=spec.name,
            name=spec.name,
            description=spec.description,
        )
        for spec in COLUMNS
    }
    return dm.ViewApply(
        space=seed.view_space,
        external_id=seed.view_external_id,
        version=seed.view_version,
        name="LIMS result",
        description="Synthetic LIMS result view used to exercise filter, limit, and aggregate pushdown.",
        properties=properties,
    )


def _iter_nodes(seed: LimsResultSeed) -> Iterator[dm.NodeApply]:
    for result_id in range(1, INSTANCE_COUNT + 1):
        yield dm.NodeApply(
            space=instance_space_for(result_id),
            external_id=external_id_for(result_id),
            sources=[dm.NodeOrEdgeData(source=seed.view, properties=properties_for(result_id).model_dump())],
        )


def _apply_with_retry(client: CogniteClient, nodes: list[dm.NodeApply]) -> None:
    for attempt in range(5):
        try:
            client.data_modeling.instances.apply(nodes=nodes)
            return
        except CogniteAPIError as exc:
            if exc.code not in {408, 429, 500, 502, 503, 504} or attempt == 4:
                raise
            time.sleep(2**attempt)


def ensure_lims_result_schema(client: CogniteClient, seed: LimsResultSeed = lims_result_seed) -> LimsResultSeed:
    """Create the data-model space, container, view, data model, and instance spaces (idempotent)."""
    client.data_modeling.spaces.apply(
        [
            dm.SpaceApply(space=seed.view_space, name="LIMS result DM DOM"),
            dm.SpaceApply(space=seed.lab_a, name="LIMS result lab A instance space"),
            dm.SpaceApply(space=seed.lab_b, name="LIMS result lab B instance space"),
        ]
    )
    client.data_modeling.containers.apply(_container_apply(seed))
    client.data_modeling.views.apply(_view_apply(seed))
    client.data_modeling.data_models.apply(
        dm.DataModelApply(
            space=seed.view_space,
            external_id=seed.model_external_id,
            version=seed.view_version,
            name="LIMS result DOM",
            description="Synthetic LIMS result data model for pygen-spark and cognite-databricks tests.",
            views=[seed.view],
        )
    )
    return seed


def aggregate_count(client: CogniteClient, seed: LimsResultSeed, instance_space: str | list[str]) -> int:
    """COUNT of nodes in ``instance_space`` for this view."""
    filter_json = build_filter_json(
        FilterSpec(
            view_space=seed.view_space,
            view_external_id=seed.view_external_id,
            view_version=seed.view_version,
            instance_space=instance_space,
        )
    )
    payload = build_aggregate_payload(
        AggregateRequestSpec(
            view_space=seed.view_space,
            view_external_id=seed.view_external_id,
            view_version=seed.view_version,
            aggregates=[AggregateMetric(fn="count", property="externalId")],
            filter_json=filter_json,
        )
    )
    url = f"/api/v1/projects/{client.config.project}/models/instances/aggregate"
    return parse_aggregate_count(client.post(url, json=payload).json())


def _fixture_is_current(client: CogniteClient, seed: LimsResultSeed) -> bool:
    retrieved = client.data_modeling.instances.retrieve(
        nodes=dm.NodeId(space=seed.lab_a, external_id=FIXTURE_EXTERNAL_ID),
        sources=[seed.view],
    )
    if not retrieved.nodes:
        return False
    stored = retrieved.nodes[0].properties.get(seed.view) or {}
    return stored.get("numericValue") == FIXTURE_NUMERIC_VALUE and stored.get("componentName") == MOISTURE_CONTENT


def _node_exists(client: CogniteClient, seed: LimsResultSeed, space: str, external_id: str) -> bool:
    retrieved = client.data_modeling.instances.retrieve(nodes=dm.NodeId(space=space, external_id=external_id))
    return len(retrieved.nodes) > 0


def ensure_lims_result_seed(
    client: CogniteClient, seed: LimsResultSeed = lims_result_seed, *, force: bool = False
) -> LimsResultSeed:
    """Upsert the schema and all 100,000 result nodes (idempotent).

    Skips the instance upsert when the fixture row and the last lab B row are already stored.
    Aggregate counts are not used here: that index lags behind retrieve.
    """
    ensure_lims_result_schema(client, seed)
    if not force and _fixture_is_current(client, seed) and _node_exists(client, seed, seed.lab_b, "result_100000"):
        print("lims result seed: already present", flush=True)
        return seed
    batch: list[dm.NodeApply] = []
    written = 0
    for node in _iter_nodes(seed):
        batch.append(node)
        if len(batch) >= APPLY_BATCH_SIZE:
            _apply_with_retry(client, batch)
            written += len(batch)
            batch = []
            if written % 5_000 == 0:
                print(f"lims result seed: {written}/{INSTANCE_COUNT}", flush=True)
    if batch:
        _apply_with_retry(client, batch)
        written += len(batch)
    print(f"lims result seed: {written}/{INSTANCE_COUNT}", flush=True)
    return seed


def main() -> None:
    """Seed cogsail from the command line."""
    path = credentials_toml_path()
    if not path.is_file():
        raise SystemExit(f"Credentials TOML not found: {path}")
    client = load_cogsail_client(path)
    if client is None:
        raise SystemExit(f"Credentials TOML is missing client_id / client_secret: {path}")
    ensure_lims_result_seed(client)
    print(
        f"{LAB_A}: {LAB_A_COUNT} nodes, {LAB_B}: {LAB_B_COUNT} nodes, view {DM_SPACE}.{VIEW_EXTERNAL_ID}/{VIEW_VERSION}"
    )


if __name__ == "__main__":
    main()
