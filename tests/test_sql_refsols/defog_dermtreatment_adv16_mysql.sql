SELECT
  (
    (
      AVG(day100_pasi_score) - AVG(day7_pasi_score)
    ) / AVG(day7_pasi_score)
  ) * 100 AS d7d100pir
FROM outcomes
WHERE
  NOT day100_pasi_score IS NULL AND NOT day7_pasi_score IS NULL
