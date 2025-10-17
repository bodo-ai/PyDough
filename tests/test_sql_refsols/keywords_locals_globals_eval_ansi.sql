SELECT
  count.node,
  cast.types,
  2.0 AS expr
FROM keywords."COUNT" AS count
JOIN keywords."CAST" AS cast
  ON cast.pk_field_name = count.this
WHERE
  count.node = 4071
