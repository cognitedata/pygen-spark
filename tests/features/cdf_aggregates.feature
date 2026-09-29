Feature: CDF instances/aggregate payloads for metric pushdown
  As a pygen-spark consumer
  I want COUNT/MIN/MAX to use instances/aggregate
  So that Spark does not over-fetch full instance pages

  Background:
    Given an aggregate view "ORCCertificate" in space "sailboat" version "v1" for instance type "node"

  Scenario: COUNT star uses externalId shorthand
    When I build an aggregate request with metrics:
      | fn    | property   |
      | count | externalId |
    Then the aggregate payload aggregates should be:
      """
      [{"count": {"property": "externalId"}}]
      """
    And the aggregate payload should reference view space "sailboat" externalId "ORCCertificate" version "v1"
    And the aggregate payload should not use sources array

  Scenario: MIN and MAX name the view property in one request
    # The view is given at the top level; CDF rejects a property path array here.
    When I build an aggregate request with metrics:
      | fn  | property |
      | min | aph_tod  |
      | max | aph_tod  |
    Then the aggregate payload aggregates should be:
      """
      [{"min": {"property": "aph_tod"}}, {"max": {"property": "aph_tod"}}]
      """

  Scenario: Filtered aggregate reuses FilterDefinition
    Given a filter equals property "boat_name" to "XBOX" on view "ORCCertificate" space "sailboat" version "v1"
    When I build an aggregate request with that filter and metrics:
      | fn    | property   |
      | count | externalId |
    Then the aggregate payload filter should equal the list filter JSON
    And the aggregate payload aggregates should be:
      """
      [{"count": {"property": "externalId"}}]
      """

  Scenario: Parse ungrouped count response
    Given an aggregate response:
      """
      {"items": [{"instanceType": "node", "aggregates": [{"aggregate": "count", "property": "externalId", "value": 12}]}]}
      """
    When I parse the aggregate count
    Then the count value should be 12

  Scenario: Parse min max response with bare property names
    Given an aggregate response:
      """
      {"items": [{"aggregates": [{"aggregate": "min", "property": "aph_tod", "value": 480.5}, {"aggregate": "max", "property": "aph_tod", "value": 530.25}]}]}
      """
    When I parse the metric aggregates
    Then the metric ("min", "aph_tod") should be 480.5
    And the metric ("max", "aph_tod") should be 530.25

  Scenario: Parse min max response with path-list property
    Given an aggregate response:
      """
      {"items": [{"aggregates": [{"aggregate": "min", "property": ["sailboat", "ORCCertificate/v1", "aph_tod"], "value": 480.5}, {"aggregate": "max", "property": ["sailboat", "ORCCertificate/v1", "aph_tod"], "value": 530.25}]}]}
      """
    When I parse the metric aggregates
    Then the metric ("min", "aph_tod") should be 480.5
    And the metric ("max", "aph_tod") should be 530.25
