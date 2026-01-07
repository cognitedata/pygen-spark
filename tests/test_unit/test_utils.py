"""Unit tests for utility functions."""

from __future__ import annotations

import pytest
from cognite.client.data_classes.data_modeling.ids import NodeId

from cognite.pygen_spark.utils import InstanceId, parse_instance_id, parse_instance_ids, to_udtf_function_name


class TestInstanceId:
    """Tests for InstanceId Pydantic model."""

    def test_from_string_valid(self) -> None:
        """Test parsing valid instance_id string."""
        instance_id = InstanceId.from_string("sailboat:ts1")
        assert instance_id.space == "sailboat"
        assert instance_id.external_id == "ts1"

    def test_from_string_with_colons_in_external_id(self) -> None:
        """Test parsing instance_id with colons in external_id."""
        instance_id = InstanceId.from_string("space:external:id:with:colons")
        assert instance_id.space == "space"
        assert instance_id.external_id == "external:id:with:colons"

    def test_from_string_empty(self) -> None:
        """Test parsing empty string raises ValueError."""
        with pytest.raises(ValueError, match="instance_id is required"):
            InstanceId.from_string("")

    def test_from_string_no_colon(self) -> None:
        """Test parsing string without colon raises ValueError."""
        with pytest.raises(ValueError, match="Invalid instance_id format"):
            InstanceId.from_string("no_colon")

    def test_to_node_id(self) -> None:
        """Test conversion to NodeId."""
        instance_id = InstanceId.from_string("sailboat:ts1")
        node_id = instance_id.to_node_id()
        assert isinstance(node_id, NodeId)
        assert node_id.space == "sailboat"
        assert node_id.external_id == "ts1"

    def test_str_representation(self) -> None:
        """Test string representation."""
        instance_id = InstanceId.from_string("sailboat:ts1")
        assert str(instance_id) == "sailboat:ts1"


class TestParseInstanceId:
    """Tests for parse_instance_id function."""

    def test_parse_valid(self) -> None:
        """Test parsing valid instance_id."""
        node_id = parse_instance_id("sailboat:ts1")
        assert isinstance(node_id, NodeId)
        assert node_id.space == "sailboat"
        assert node_id.external_id == "ts1"

    def test_parse_invalid(self) -> None:
        """Test parsing invalid instance_id raises ValueError."""
        with pytest.raises(ValueError):
            parse_instance_id("invalid")


class TestParseInstanceIds:
    """Tests for parse_instance_ids function."""

    def test_parse_single(self) -> None:
        """Test parsing single instance_id."""
        node_ids = parse_instance_ids("sailboat:ts1")
        assert len(node_ids) == 1
        assert node_ids[0].space == "sailboat"
        assert node_ids[0].external_id == "ts1"

    def test_parse_multiple(self) -> None:
        """Test parsing multiple instance_ids."""
        node_ids = parse_instance_ids("sailboat:ts1,otherspace:ts2")
        assert len(node_ids) == 2
        assert node_ids[0].space == "sailboat"
        assert node_ids[0].external_id == "ts1"
        assert node_ids[1].space == "otherspace"
        assert node_ids[1].external_id == "ts2"

    def test_parse_with_whitespace(self) -> None:
        """Test parsing with whitespace is handled."""
        node_ids = parse_instance_ids("sailboat:ts1 , otherspace:ts2")
        assert len(node_ids) == 2

    def test_parse_empty(self) -> None:
        """Test parsing empty string raises ValueError."""
        with pytest.raises(ValueError, match="instance_ids is required"):
            parse_instance_ids("")

    def test_parse_only_whitespace(self) -> None:
        """Test parsing only whitespace raises ValueError."""
        with pytest.raises(ValueError, match="At least one valid instance_id"):
            parse_instance_ids("  ,  ,  ")


class TestToUdtfFunctionName:
    """Tests for to_udtf_function_name function."""

    def test_simple_case(self) -> None:
        """Test simple camelCase conversion."""
        assert to_udtf_function_name("SmallBoat") == "small_boat_udtf"

    def test_already_snake_case(self) -> None:
        """Test already snake_case input."""
        assert to_udtf_function_name("small_boat") == "small_boat_udtf"

    def test_already_has_udtf_suffix(self) -> None:
        """Test input already ending with _udtf."""
        assert to_udtf_function_name("small_boat_udtf") == "small_boat_udtf"

    def test_with_numbers(self) -> None:
        """Test conversion with numbers."""
        assert to_udtf_function_name("Boat3D") == "boat_3d_udtf"

    def test_all_caps(self) -> None:
        """Test all caps conversion."""
        assert to_udtf_function_name("HTTPResponse") == "http_response_udtf"
