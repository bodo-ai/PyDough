SELECT
  (
    (
      AVG(CAST(day100_pasi_score AS DOUBLE PRECISION)) - AVG(CAST(day7_pasi_score AS DOUBLE PRECISION))
    ) / NULLIF(AVG(CAST(day7_pasi_score AS DOUBLE PRECISION)), 0)
  ) * 100 AS d7d100pir
FROM MAIN.OUTCOMES
WHERE
  NOT day100_pasi_score IS NULL AND NOT day7_pasi_score IS NULL
