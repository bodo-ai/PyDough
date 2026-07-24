SELECT
  (
    CAST((
      AVG(CAST(day100_pasi_score AS REAL)) - AVG(CAST(day7_pasi_score AS REAL))
    ) AS REAL) / NULLIF(AVG(CAST(day7_pasi_score AS REAL)), 0)
  ) * 100 AS d7d100pir
FROM main.outcomes
WHERE
  NOT day100_pasi_score IS NULL AND NOT day7_pasi_score IS NULL
