SELECT
  NULLIF(COUNT(*), 0) AS n_transactions,
  COALESCE(SUM(sbtransaction.sbtxamount), 0) AS total_amount
FROM mysql.broker.sbtransaction AS sbtransaction
JOIN postgres.main.sbcustomer AS sbcustomer
  ON LOWER(sbcustomer.sbcustcountry) = 'usa'
  AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
WHERE
  sbtransaction.sbtxdatetime < DATE_TRUNC(
    'DAY',
    DATE_ADD(
      'DAY',
      (
        (
          DAY_OF_WEEK(CURRENT_TIMESTAMP) - 1
        ) % 7
      ) * -1,
      CURRENT_TIMESTAMP
    )
  )
  AND sbtransaction.sbtxdatetime >= DATE_ADD(
    'DAY',
    -7,
    DATE_TRUNC(
      'DAY',
      DATE_ADD(
        'DAY',
        (
          (
            DAY_OF_WEEK(CURRENT_TIMESTAMP) - 1
          ) % 7
        ) * -1,
        CURRENT_TIMESTAMP
      )
    )
  )
