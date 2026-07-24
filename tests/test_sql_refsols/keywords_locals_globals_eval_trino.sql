SELECT
  "count".node,
  "cast".types,
  2.0 AS expr
FROM keywords."count" AS "count"
JOIN keywords."cast" AS "cast"
  ON "cast".pk_field_name = "count".this
WHERE
  "count".node = 4071
