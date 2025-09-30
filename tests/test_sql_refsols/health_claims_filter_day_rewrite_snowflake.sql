SELECT
  COUNT(*) AS n
FROM bodo.health.claims
WHERE
  DAY(CAST(PTY_UNPROTECT_DOB(claim_date) AS TIMESTAMP)) = 31
