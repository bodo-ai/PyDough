WITH "_t1" AS (
  SELECT
    "table"."a" AS "a"
  FROM "table" AS "table"
)
SELECT
  "_t0"."a" AS "a"
FROM "_t1" AS "_t0"
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM "_t1" AS "_t1"
    WHERE
      "_t0"."a" = "_t1"."a"
  )
