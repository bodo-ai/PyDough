SELECT
  COUNT(*) AS n
FROM bodo.health.claims
WHERE
  YEAR(CAST(claim_date AS TIMESTAMP)) > 2020
