SELECT
  sbtxtype AS transaction_type,
  COUNT(DISTINCT sbtxcustid) AS num_customers,
  AVG(sbtxshares) AS avg_shares
FROM MAIN.SBTRANSACTION
WHERE
  sbtxdatetime <= TO_DATE('2023-03-31', 'YYYY-MM-DD')
  AND sbtxdatetime >= TO_DATE('2023-01-01', 'YYYY-MM-DD')
GROUP BY
  sbtxtype
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
FETCH FIRST 3 ROWS ONLY
