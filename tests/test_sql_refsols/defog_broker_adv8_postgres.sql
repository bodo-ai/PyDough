SELECT
  NULLIF(COUNT(*), 0) AS n_transactions,
  COALESCE(SUM(sbtransaction.sbtxamount), 0) AS total_amount
FROM main.sbtransaction AS sbtransaction
JOIN main.sbcustomer AS sbcustomer
  ON LOWER(sbcustomer.sbcustcountry) = 'usa'
  AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
WHERE
  sbtransaction.sbtxdatetime < DATE_TRUNC(
    'DAY',
    CURRENT_TIMESTAMP - CAST((
      EXTRACT(DOW FROM CURRENT_TIMESTAMP) + 6
    ) % 7 || ' days' AS INTERVAL)
  )
  AND sbtransaction.sbtxdatetime >= DATE_TRUNC(
    'DAY',
    CURRENT_TIMESTAMP - CAST((
      EXTRACT(DOW FROM CURRENT_TIMESTAMP) + 6
    ) % 7 || ' days' AS INTERVAL)
  ) - INTERVAL '1 WEEK'
