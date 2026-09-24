WITH "quoted-name" AS (
  SELECT
    SEQ4() AS "name space"
  FROM TABLE(GENERATOR(ROWCOUNT => 5)) AS _0
)
SELECT
  "name space"
FROM "quoted-name"
