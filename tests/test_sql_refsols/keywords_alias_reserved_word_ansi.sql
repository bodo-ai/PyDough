SELECT
  COALESCE("WHERE".default_to, "WHERE".".CALCULATE") AS calculate2,
  calculate.".WHERE" AS "_where",
  calculate."LIKE" AS "_like",
  calculate.datetime,
  "WHERE".abs,
  "WHERE".has
FROM keywords."WHERE" AS "WHERE"
JOIN keywords.calculate AS calculate
  ON "WHERE".".CALCULATE" = calculate.".WHERE"
WHERE
  "WHERE".".CALCULATE" = 4 AND "WHERE".present IS NULL
