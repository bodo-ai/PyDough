SELECT
  COALESCE("where".default_to, "where".".CALCULATE") AS calculate2,
  calculate.".WHERE" AS "_where",
  calculate."LIKE" AS "_like",
  calculate.datetime,
  "where".abs,
  "where".has
FROM keywords."WHERE" AS "where"
JOIN keywords.calculate AS calculate
  ON "where".".CALCULATE" = calculate.".WHERE"
WHERE
  "where".".CALCULATE" = 4 AND "where".present IS NULL
