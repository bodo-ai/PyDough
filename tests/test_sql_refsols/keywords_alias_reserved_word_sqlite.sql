SELECT
  COALESCE("where".default_to, "where".".calculate") AS calculate2,
  calculate.".where" AS _where,
  calculate."like" AS _like,
  calculate.datetime,
  "where".abs,
  "where".has
FROM keywords."where" AS "where"
JOIN keywords.calculate AS calculate
  ON "where".".calculate" = calculate.".where"
WHERE
  "where".".calculate" = 4 AND "where".present IS NULL
