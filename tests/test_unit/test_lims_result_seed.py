"""Invariants of the synthetic LIMS result generator. No CDF client."""

from __future__ import annotations

from tests.test_live.lims_result import (
    _INDEXED,
    ALIQUOT_ABSENT_COUNT,
    COLUMNS,
    DM_SPACE,
    FIXTURE_EXTERNAL_ID,
    FIXTURE_ID,
    FIXTURE_NUMERIC_VALUE,
    INSTANCE_COUNT,
    LAB_A,
    LAB_A_COUNT,
    LAB_B,
    LAB_B_COUNT,
    LONG_COMMENT_LENGTH,
    MAX_SPEC_LIMIT,
    MOISTURE_CONTENT,
    MOISTURE_CONTENT_COUNT,
    OUT_OF_SPEC_COUNT,
    OUT_OF_SPEC_NUMERIC_VALUE,
    RESULT_TYPES,
    external_id_for,
    instance_space_for,
    is_out_of_spec,
    january_entered_count,
    numeric_bounds,
    numeric_value_for,
    properties_for,
    result_type_for,
)


def test_schema_has_the_forty_lims_columns() -> None:
    assert len(COLUMNS) == 40
    assert len({spec.name for spec in COLUMNS}) == 40
    assert set(_INDEXED) <= {spec.name for spec in COLUMNS}
    required = {spec.name for spec in COLUMNS if spec.required}
    assert required == {
        "resultId",
        "testId",
        "sampleId",
        "componentName",
        "resultType",
        "analyticalMethodId",
        "dilutionFactor",
        "statusCode",
        "isReportable",
        "isOutOfSpec",
        "isOutOfTrend",
        "retestFlag",
        "rowVersionChecksum",
    }


def test_population_counts_match_the_documented_distribution() -> None:
    ids = range(1, INSTANCE_COUNT + 1)
    assert sum(
        1 for result_id in ids if properties_for(result_id).model_dump()["componentName"] == MOISTURE_CONTENT
    ) == (MOISTURE_CONTENT_COUNT)
    assert sum(1 for result_id in ids if is_out_of_spec(result_id)) == OUT_OF_SPEC_COUNT
    assert sum(1 for result_id in ids if properties_for(result_id).model_dump()["aliquotId"] is None) == (
        ALIQUOT_ABSENT_COUNT
    )
    assert sum(1 for result_id in ids if instance_space_for(result_id) == LAB_A) == LAB_A_COUNT
    assert sum(1 for result_id in ids if instance_space_for(result_id) == LAB_B) == LAB_B_COUNT
    assert {result_type_for(result_id) for result_id in ids} == set(RESULT_TYPES)


def test_fixture_row_is_fully_populated_and_in_lab_a() -> None:
    dumped = properties_for(FIXTURE_ID).model_dump()

    assert external_id_for(FIXTURE_ID) == FIXTURE_EXTERNAL_ID
    assert instance_space_for(FIXTURE_ID) == LAB_A
    assert DM_SPACE not in {LAB_A, LAB_B}
    assert all(value is not None for value in dumped.values())
    assert dumped["componentName"] == MOISTURE_CONTENT
    assert dumped["resultType"] == "NUMERIC"
    assert dumped["numericValue"] == FIXTURE_NUMERIC_VALUE
    assert dumped["isOutOfSpec"] is False
    assert dumped["statusCode"] == "AUTHORIZED"
    assert dumped["numericValue"] < MAX_SPEC_LIMIT


def test_null_numeric_rows_and_out_of_spec_values() -> None:
    text_row = properties_for(70).model_dump()
    assert text_row["resultType"] == "TEXT"
    assert text_row["numericValue"] is None

    out_of_spec = properties_for(100).model_dump()
    assert out_of_spec["isOutOfSpec"] is True
    assert out_of_spec["numericValue"] == OUT_OF_SPEC_NUMERIC_VALUE
    assert out_of_spec["numericValue"] > MAX_SPEC_LIMIT
    assert len(properties_for(1000).model_dump()["auditComment"]) == LONG_COMMENT_LENGTH


def test_numeric_bounds_and_january_window_are_stable() -> None:
    assert numeric_bounds(LAB_A) == (1.0, OUT_OF_SPEC_NUMERIC_VALUE)
    assert numeric_bounds(LAB_B) == (1.0, OUT_OF_SPEC_NUMERIC_VALUE)
    assert numeric_value_for(70) is None
    assert january_entered_count() == sum(1 for result_id in range(1, INSTANCE_COUNT + 1) if result_id % 366 <= 30)
