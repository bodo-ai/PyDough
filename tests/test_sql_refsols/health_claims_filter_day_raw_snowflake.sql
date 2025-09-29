SELECT
  COUNT(*) AS n
FROM bodo.health.claims
WHERE
  DAY(CAST(claim_date AS TIMESTAMP)) = 31
