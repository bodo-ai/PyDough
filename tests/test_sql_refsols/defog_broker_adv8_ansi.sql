SELECT
  CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE NULL END AS n_transactions,
  COALESCE(SUM(sbtxamount), 0) AS total_amount
FROM main.sbtransaction
WHERE
  sbtxdatetime < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
  AND sbtxdatetime >= DATE_SUB(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), 1, WEEK)
