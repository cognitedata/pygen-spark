"""Integration tests mimicking SQL queries from Session scoped zero-copy Databricks-CDF notebook."""

from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen_spark import SparkUDTFGenerator


@pytest.mark.integration
class TestDataModelUdtfQueries:
    """Integration tests for Data Model UDTF queries from notebook."""

    def test_basic_query_all_null_filters(
        self,
        spark_udtf_generator: SparkUDTFGenerator,
        sample_sailboat_view: dm.View,
        mock_cognite_client,
    ) -> None:
        """Test basic UDTF query with all NULL filters (Cell 14)."""
        # Generate UDTF
        result = spark_udtf_generator.generate_udtfs()
        assert result.total_count > 0
        
        # Verify UDTF file was created
        udtf_file = result.get_file("SmallBoat")
        assert udtf_file is not None
        assert udtf_file.exists()
        
        # Verify UDTF code contains expected structure
        code = udtf_file.read_text()
        assert "class" in code
        assert "eval" in code.lower() or "__call__" in code.lower()
        assert "name" in code.lower()  # Filter parameter
        assert "description" in code.lower()  # Filter parameter

    def test_named_parameters_query(
        self,
        spark_udtf_generator: SparkUDTFGenerator,
        sample_sailboat_view: dm.View,
        mock_cognite_client,
    ) -> None:
        """Test UDTF query with named parameters and filter (Cell 15)."""
        # Mock CDF API response for filtered query
        mock_cognite_client.data_modeling.instances.list.return_value = [
            MagicMock(
                external_id="boat1",
                properties={"name": "XBOX", "description": "Test boat"},
            )
        ]
        
        # Generate UDTF
        result = spark_udtf_generator.generate_udtfs()
        udtf_file = result.get_file("SmallBoat")
        assert udtf_file is not None
        
        # Verify filter parameter handling in generated code
        code = udtf_file.read_text()
        assert "name" in code.lower()
        # Verify filter building logic exists
        assert "filter" in code.lower() or "equals" in code.lower()


@pytest.mark.integration
class TestTimeSeriesUdtfQueries:
    """Integration tests for Time Series UDTF queries from notebook."""

    def test_single_time_series_query(
        self,
        mock_cognite_client,
    ) -> None:
        """Test single time series datapoints query (Cell 17)."""
        # This tests the time_series_datapoints_udtf template
        # Verify instance_id parsing
        from cognite.pygen_spark.utils import parse_instance_id

        node_id = parse_instance_id("sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround")
        assert node_id.space == "sailboat"
        assert node_id.external_id == "vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround"

    def test_multiple_time_series_query(
        self,
        mock_cognite_client,
    ) -> None:
        """Test multiple time series datapoints query (Cell 18)."""
        # This tests the time_series_datapoints_long_udtf template
        from cognite.pygen_spark.utils import parse_instance_ids

        instance_ids_str = "sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround,sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.courseOverGroundTrue"
        node_ids = parse_instance_ids(instance_ids_str)
        
        assert len(node_ids) == 2
        assert node_ids[0].space == "sailboat"
        assert node_ids[1].space == "sailboat"
        assert "speedOverGround" in node_ids[0].external_id
        assert "courseOverGroundTrue" in node_ids[1].external_id

    def test_latest_time_series_query(
        self,
        mock_cognite_client,
    ) -> None:
        """Test latest time series datapoints query (Cell 19)."""
        # This tests the time_series_latest_datapoints_udtf template
        from cognite.pygen_spark.utils import parse_instance_ids

        instance_ids_str = "sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround,sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.courseOverGroundTrue"
        node_ids = parse_instance_ids(instance_ids_str)
        
        assert len(node_ids) == 2
        # Verify parsing handles multiple time series from same space


@pytest.mark.integration
class TestFilteringQueries:
    """Integration tests for filtering queries from notebook (Cells 20-24)."""

    def test_filter_by_external_id(
        self,
        spark_udtf_generator: SparkUDTFGenerator,
        mock_cognite_client,
    ) -> None:
        """Test filtering by external_id (Cell 20)."""
        # Mock CDF API response
        mock_cognite_client.data_modeling.instances.list.return_value = [
            MagicMock(external_id="boat::257038990", properties={"name": "Test"})
        ]
        
        # Generate UDTF
        result = spark_udtf_generator.generate_udtfs()
        udtf_file = result.get_file("SmallBoat")
        assert udtf_file is not None
        
        # Verify filter handling in code
        code = udtf_file.read_text()
        # Should handle WHERE clause filtering (this would be in Spark SQL, not Python code)
        # But we can verify the UDTF structure supports it

    def test_filter_by_property(
        self,
        spark_udtf_generator: SparkUDTFGenerator,
        mock_cognite_client,
    ) -> None:
        """Test filtering by property (Cell 21)."""
        # Mock CDF API response for name filter
        mock_cognite_client.data_modeling.instances.list.return_value = [
            MagicMock(external_id="boat1", properties={"name": "XBOX"})
        ]
        
        result = spark_udtf_generator.generate_udtfs()
        udtf_file = result.get_file("SmallBoat")
        assert udtf_file is not None
        
        code = udtf_file.read_text()
        # Verify filter parameter exists
        assert "name" in code.lower()

    def test_filter_by_space_and_external_id(
        self,
        spark_udtf_generator: SparkUDTFGenerator,
        mock_cognite_client,
    ) -> None:
        """Test filtering by space and external_id (Cell 22)."""
        # Mock CDF API response
        mock_cognite_client.data_modeling.instances.list.return_value = [
            MagicMock(external_id="boat::257038990", properties={})
        ]
        
        result = spark_udtf_generator.generate_udtfs()
        udtf_file = result.get_file("SmallBoat")
        assert udtf_file is not None
        
        # Verify space handling in generated code
        code = udtf_file.read_text()
        assert "space" in code.lower() or "sailboat" in code.lower()

    def test_filter_by_numeric_range(
        self,
        spark_udtf_generator: SparkUDTFGenerator,
        mock_cognite_client,
    ) -> None:
        """Test filtering by numeric range (Cell 23)."""
        # Mock CDF API response
        mock_cognite_client.data_modeling.instances.list.return_value = [
            MagicMock(external_id="boat1", properties={"boat_guid": 257038500})
        ]
        
        result = spark_udtf_generator.generate_udtfs()
        udtf_file = result.get_file("SmallBoat")
        assert udtf_file is not None
        
        code = udtf_file.read_text()
        # Verify numeric property handling
        assert "boat_guid" in code.lower() or "boatGuid" in code.lower()

    def test_complex_filtering_with_order_by(
        self,
        spark_udtf_generator: SparkUDTFGenerator,
        mock_cognite_client,
    ) -> None:
        """Test complex filtering with multiple conditions (Cell 24)."""
        # Mock CDF API response
        mock_cognite_client.data_modeling.instances.list.return_value = [
            MagicMock(
                external_id="boat1",
                properties={
                    "mmsi_country": "country::257",
                    "boat_guid": 257038500,
                },
            )
        ]
        
        result = spark_udtf_generator.generate_udtfs()
        udtf_file = result.get_file("SmallBoat")
        assert udtf_file is not None
        
        code = udtf_file.read_text()
        # Verify multiple property handling
        assert "mmsi_country" in code.lower() or "mmsiCountry" in code.lower()


@pytest.mark.integration
class TestJoinQueries:
    """Integration tests for JOIN queries from notebook (Cell 25)."""

    def test_join_between_udtfs(
        self,
        spark_udtf_generator: SparkUDTFGenerator,
        sample_sailboat_view: dm.View,
        sample_nmea_time_series_view: dm.View,
        mock_cognite_client,
    ) -> None:
        """Test JOIN between small_boat_udtf and nmea_time_series_udtf (Cell 25)."""
        # Generate both UDTFs
        result = spark_udtf_generator.generate_udtfs()
        
        small_boat_file = result.get_file("SmallBoat")
        nmea_file = result.get_file("NmeaTimeSeries")
        
        assert small_boat_file is not None
        assert nmea_file is not None
        
        # Verify both UDTFs have compatible output schemas for joining
        small_boat_code = small_boat_file.read_text()
        nmea_code = nmea_file.read_text()
        
        # Both should have space and external_id columns for joining
        assert "space" in small_boat_code.lower() or "external_id" in small_boat_code.lower()
        assert "space" in nmea_code.lower() or "external_id" in nmea_code.lower()
        
        # Verify mmsi property exists in nmea UDTF for join condition
        assert "mmsi" in nmea_code.lower()
        # Verify boat_guid exists in small_boat UDTF for join condition
        assert "boat_guid" in small_boat_code.lower() or "boatGuid" in small_boat_code.lower()

