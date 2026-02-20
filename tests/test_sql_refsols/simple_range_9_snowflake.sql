WITH "quoted-name" AS (
  SELECT
    SEQ4() AS "name space"
  FROM TABLE(GENERATOR(ROWCOUNT => 5))
)
SELECT
  "name space"
FROM "quoted-name"
