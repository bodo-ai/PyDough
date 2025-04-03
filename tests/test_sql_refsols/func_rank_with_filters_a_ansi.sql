WITH "_t0" AS (
  SELECT
    RANK() OVER (ORDER BY "table"."a") AS "r",
    "table"."a" AS "a",
    "table"."b" AS "b"
  FROM "table" AS "table"
  WHERE
    "table"."b" = 0
)
SELECT
  "_t0"."a" AS "a",
  "_t0"."b" AS "b",
  "_t0"."r" AS "r"
FROM "_t0" AS "_t0"
WHERE
  "_t0"."r" >= 3
