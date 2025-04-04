WITH "_t0" AS (
  SELECT
    "table"."a" AS "a"
  FROM "table" AS "table"
)
SELECT
  "_t0"."a" AS "a"
FROM "_t0" AS "_t0"
JOIN "_t0" AS "_t1"
  ON "_t0"."a" = "_t1"."a"
JOIN "_t0" AS "_t2"
  ON "_t0"."a" = "_t2"."a"
