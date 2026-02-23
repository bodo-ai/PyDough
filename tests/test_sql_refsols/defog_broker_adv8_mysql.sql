SELECT
  NULLIF(COUNT(*), 0) AS n_transactions,
  COALESCE(SUM(sbTransaction.sbtxamount), 0) AS total_amount
FROM broker.sbTransaction AS sbTransaction
JOIN broker.sbCustomer AS sbCustomer
  ON LOWER(sbCustomer.sbcustcountry) = 'usa'
  AND sbCustomer.sbcustid = sbTransaction.sbtxcustid
WHERE
  sbTransaction.sbtxdatetime < CAST(DATE_SUB(
    CURRENT_TIMESTAMP(),
    INTERVAL (
      (
        DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
      ) % 7
    ) DAY
  ) AS DATE)
  AND sbTransaction.sbtxdatetime >= DATE_SUB(
    CAST(DATE_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      ) DAY
    ) AS DATE),
    INTERVAL '1' WEEK
  )
