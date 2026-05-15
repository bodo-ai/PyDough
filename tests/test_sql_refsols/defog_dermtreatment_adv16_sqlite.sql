SELECT
  (
    CAST((
      AVG(day100_pasi_score) - AVG(day7_pasi_score)
    ) AS REAL) / NULLIF(AVG(day7_pasi_score), 0)
  ) * 100 AS d7d100pir
FROM main.outcomes
WHERE
  NOT day100_pasi_score IS NULL AND NOT day7_pasi_score IS NULL
