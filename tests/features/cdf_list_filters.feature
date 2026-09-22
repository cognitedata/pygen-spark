Feature: CDF list filter JSON for data-model UDTFs
  As a pygen-spark consumer
  I want generated UDTFs to emit exact CDF FilterDefinition JSON
  So that WHERE predicates push down to instances/list correctly

  Background:
    Given a view "LimsResults" in space "sp-lims" version "v1" for instance type "node"
    And property "TestSeqNumber" is a scalar
    And property "DilutionFactor" is a scalar
    And property "tags" is an array

  Scenario: Scalar equals builds CDF equals filter
    When I build a filter with property "TestSeqNumber" equal to "5889450"
    Then the filter JSON should be:
      """
      {"equals": {"property": ["sp-lims", "LimsResults/v1", "TestSeqNumber"], "value": "5889450"}}
      """

  Scenario: List value builds CDF in filter
    When I build a filter with property "TestSeqNumber" equal to list '["a", "b"]'
    Then the filter JSON should be:
      """
      {"in": {"property": ["sp-lims", "LimsResults/v1", "TestSeqNumber"], "values": ["a", "b"]}}
      """

  Scenario: Array property builds containsAny filter
    When I build a filter with property "tags" equal to "alpha"
    Then the filter JSON should be:
      """
      {"containsAny": {"property": ["sp-lims", "LimsResults/v1", "tags"], "values": ["alpha"]}}
      """

  Scenario: IS NOT NULL builds CDF exists filter
    When I build a filter with exists properties '["DilutionFactor"]'
    Then the filter JSON should be:
      """
      {"exists": {"property": ["sp-lims", "LimsResults/v1", "DilutionFactor"]}}
      """

  Scenario: IS NULL builds CDF not-exists filter
    When I build a filter with not-exists properties '["DilutionFactor"]'
    Then the filter JSON should be:
      """
      {"not": {"exists": {"property": ["sp-lims", "LimsResults/v1", "DilutionFactor"]}}}
      """

  Scenario: Equals and exists combine with and
    When I build a filter with property "TestSeqNumber" equal to "5889450"
    And I add exists properties '["DilutionFactor"]'
    Then the filter JSON should be:
      """
      {"and": [{"equals": {"property": ["sp-lims", "LimsResults/v1", "TestSeqNumber"], "value": "5889450"}}, {"exists": {"property": ["sp-lims", "LimsResults/v1", "DilutionFactor"]}}]}
      """

  Scenario: Instance space equals builds node space filter
    When I build a filter with instance space "sp-lims-instances"
    Then the filter JSON should be:
      """
      {"equals": {"property": ["node", "space"], "value": "sp-lims-instances"}}
      """

  Scenario: Instance space IN builds node space in filter
    When I build a filter with instance space list '["space-a", "space-b"]'
    Then the filter JSON should be:
      """
      {"in": {"property": ["node", "space"], "values": ["space-a", "space-b"]}}
      """

  Scenario: External id equals builds node externalId filter
    When I build a filter with external id "my-id"
    Then the filter JSON should be:
      """
      {"equals": {"property": ["node", "externalId"], "value": "my-id"}}
      """

  Scenario: Range gt builds CDF range filter
    When I build a filter with gt map '{"boat_guid": 100}'
    Then the filter JSON should be:
      """
      {"range": {"property": ["sp-lims", "LimsResults/v1", "boat_guid"], "gt": 100}}
      """

  Scenario: Combined range gte and lt on same property
    When I build a filter with gte map '{"boat_guid": 10}'
    And I add lt map '{"boat_guid": 20}'
    Then the filter JSON should be:
      """
      {"range": {"property": ["sp-lims", "LimsResults/v1", "boat_guid"], "gte": 10, "lt": 20}}
      """

  Scenario: No filters omits filter key
    When I build a filter with no conditions
    Then the filter JSON should be null
