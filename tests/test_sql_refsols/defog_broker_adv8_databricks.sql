SELECT
  NULLIF(COUNT(*), 0) AS n_transactions,
  COALESCE(SUM(sbtransaction.sbtxamount), 0) AS total_amount
FROM defog.broker.sbtransaction AS sbtransaction
JOIN defog.broker.sbcustomer AS sbcustomer
  ON LOWER(sbcustomer.sbcustcountry) = 'usa'
  AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
WHERE
  sbtransaction.sbtxdatetime < DATE_ADD(
    CAST(CURRENT_TIMESTAMP() AS DATE),
    -(
      (
        DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
      ) % 7
    )
  )
  AND sbtransaction.sbtxdatetime >= DATE_ADD(
    DATE_ADD(
      CAST(CURRENT_TIMESTAMP() AS DATE),
      -(
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      )
    ),
    -7
  )
