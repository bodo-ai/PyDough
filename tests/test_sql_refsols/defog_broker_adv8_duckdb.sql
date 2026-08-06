SELECT
  NULLIF(COUNT(*), 0) AS n_transactions,
  COALESCE(SUM(sbtransaction.sbtxamount), 0) AS total_amount
FROM main.sbtransaction AS sbtransaction
JOIN main.sbcustomer AS sbcustomer
  ON LOWER(sbcustomer.sbcustcountry) = 'usa'
  AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
WHERE
  sbtransaction.sbtxdatetime < (
    CAST(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS DATE) - CAST((
      (
        DAYOFWEEK(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)) + 6
      ) % 7
    ) AS INT)
  )
  AND sbtransaction.sbtxdatetime >= CAST(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS DATE) - CAST((
    (
      DAYOFWEEK(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)) + 6
    ) % 7
  ) AS INT) - 7 * INTERVAL '1' DAY
