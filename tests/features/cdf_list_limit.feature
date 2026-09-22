Feature: CDF list LIMIT pushdown for data-model UDTFs
  As a pygen-spark consumer
  I want SQL LIMIT to control instances/list page size and pagination
  So that we do not over-fetch from CDF

  Scenario: Row limit 1 sets list API limit 1
    Given page_limit is 1000 and row_limit is 1 and rows_yielded is 0
    When I compute the effective list request limit
    Then the request limit should be 1

  Scenario: Row limit None preserves page_limit
    Given page_limit is 250 and row_limit is None and rows_yielded is 0
    When I compute the effective list request limit
    Then the request limit should be 250

  Scenario: Row limit stops when enough rows yielded
    Given page_limit is 1000 and row_limit is 10 and rows_yielded is 10
    When I compute the effective list request limit
    Then the request limit should be null

  Scenario: Remaining rows cap page size
    Given page_limit is 1000 and row_limit is 10 and rows_yielded is 7
    When I compute the effective list request limit
    Then the request limit should be 3
