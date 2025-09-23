SELECT
  CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE NULL END AS n_transactions,
  COALESCE(SUM(sbtxamount), 0) AS total_amount
FROM main.sbtransaction
WHERE
  sbtxdatetime < DATE_TRUNC(
    'DAY',
    CURRENT_TIMESTAMP - CAST((
      EXTRACT(DOW FROM CURRENT_TIMESTAMP) + 6
    ) % 7 || ' days' AS INTERVAL)
  )
  AND sbtxdatetime >= DATE_TRUNC(
    'DAY',
    CURRENT_TIMESTAMP - CAST((
      EXTRACT(DOW FROM CURRENT_TIMESTAMP) + 6
    ) % 7 || ' days' AS INTERVAL)
  ) - INTERVAL '1 WEEK'
