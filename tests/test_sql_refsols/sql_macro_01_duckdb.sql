WITH _0 AS (
  SELECT
    '1-URGENT' AS priority_name,
    1 AS priority_value
  UNION ALL
  SELECT
    '2-HIGH' AS "2-high",
    2 AS "2"
  UNION ALL
  SELECT
    '3-MEDIUM' AS "3-medium",
    3 AS "3"
  UNION ALL
  SELECT
    '4-NOT SPECIFIED' AS "4-not specified",
    4 AS "4"
  UNION ALL
  SELECT
    '5-LOW' AS "5-low",
    5 AS "5"
)
SELECT
  priority_name AS name,
  priority_value AS value
FROM _0
