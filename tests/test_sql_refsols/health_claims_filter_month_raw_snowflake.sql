SELECT
  COUNT(*) AS n
FROM bodo.health.claims
WHERE
  MONTH(CAST(claim_date AS TIMESTAMP)) = 12
