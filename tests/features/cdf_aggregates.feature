Feature: CDF instances/aggregate payloads for metric pushdown
  As a pygen-spark consumer
  I want COUNT/MIN/MAX to use instances/aggregate
  So that Spark does not over-fetch full instance pages

  Background:
    Given an aggregate view "LimsResults" in space "sp-lims" version "v1" for instance type "node"

  Scenario: COUNT star uses externalId shorthand
    When I build an aggregate request with metrics:
      | fn    | property   |
      | count | externalId |
    Then the aggregate payload aggregates should be:
      """
      [{"count": {"property": "externalId"}}]
      """
    And the aggregate payload should reference view space "sp-lims" externalId "LimsResults" version "v1"
    And the aggregate payload should not use sources array

  Scenario: MIN and MAX use full property paths in one request
    When I build an aggregate request with metrics:
      | fn  | property       |
      | min | DateAuthorised |
      | max | DateAuthorised |
    Then the aggregate payload aggregates should be:
      """
      [{"min": {"property": ["sp-lims", "LimsResults/v1", "DateAuthorised"]}}, {"max": {"property": ["sp-lims", "LimsResults/v1", "DateAuthorised"]}}]
      """

  Scenario: Filtered aggregate reuses FilterDefinition
    Given a filter equals property "TestSeqNumber" to "5889450" on view "LimsResults" space "sp-lims" version "v1"
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

  Scenario: Parse min max response with path-list property
    Given an aggregate response:
      """
      {"items": [{"aggregates": [{"aggregate": "min", "property": ["sp-lims", "LimsResults/v1", "DateAuthorised"], "value": 1704067200000}, {"aggregate": "max", "property": ["sp-lims", "LimsResults/v1", "DateAuthorised"], "value": 1735689600000}]}]}
      """
    When I parse the metric aggregates
    Then the metric ("min", "DateAuthorised") should be 1704067200000
    And the metric ("max", "DateAuthorised") should be 1735689600000
