SELECT
  (
    (
      AVG(CAST(CAST(day100_pasi_score AS DOUBLE PRECISION) AS DECIMAL)) - AVG(CAST(CAST(day7_pasi_score AS DOUBLE PRECISION) AS DECIMAL))
    ) / NULLIF(AVG(CAST(CAST(day7_pasi_score AS DOUBLE PRECISION) AS DECIMAL)), 0)
  ) * 100 AS d7d100pir
FROM main.outcomes
WHERE
  NOT day100_pasi_score IS NULL AND NOT day7_pasi_score IS NULL
