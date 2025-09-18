SELECT
  COALESCE(where.default_to, ".calculate") AS calculate,
  ".where" AS _where,
  "like" AS _like,
  calculate.datetime,
  where.abs,
  where.has
FROM keywords."where" AS where
JOIN keywords.calculate AS calculate
  ON calculate.".where" = where.".calculate"
WHERE
  where.".calculate" = 4 AND where.present IS NULL
