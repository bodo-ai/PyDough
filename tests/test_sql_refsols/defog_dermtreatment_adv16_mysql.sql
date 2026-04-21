SELECT
  (
    (
      AVG(CAST(day100_pasi_score AS DOUBLE)) - AVG(CAST(day7_pasi_score AS DOUBLE))
    ) / NULLIF(AVG(CAST(day7_pasi_score AS DOUBLE)), 0)
  ) * 100 AS d7d100pir
FROM outcomes
WHERE
  NOT day100_pasi_score IS NULL AND NOT day7_pasi_score IS NULL
