WITH "_t0" AS (
  SELECT
    "table"."a" AS "a"
  FROM "table" AS "table"
)
SELECT
  "_t0"."a" AS "a"
FROM "_t0" AS "_t0"
RIGHT JOIN "_t0" AS "_t1"
  ON "_t0"."a" = "_t1"."a"
