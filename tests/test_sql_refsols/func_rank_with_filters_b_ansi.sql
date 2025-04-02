WITH "_t1" AS (
  SELECT
    RANK() OVER (ORDER BY "table"."a") AS "r",
    "table"."a" AS "a",
    "table"."b" AS "b"
  FROM "table" AS "table"
)
SELECT
  "_t1"."a" AS "a",
  "_t1"."b" AS "b",
  "_t1"."r" AS "r"
FROM "_t1" AS "_t1"
WHERE
  "_t1"."b" = 0 AND "_t1"."r" >= 3
