WITH "_t0" AS (
  SELECT
    "table"."a" AS "a",
    "table"."b" AS "b"
  FROM "table" AS "table"
  ORDER BY
    "a"
  LIMIT 5
)
SELECT
  "_t0"."a" AS "a",
  "_t0"."b" AS "b"
FROM "_t0" AS "_t0"
ORDER BY
  "b" DESC
LIMIT 2
