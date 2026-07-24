SELECT
  NULLIF(COUNT(*), 0) AS n_transactions,
  COALESCE(SUM(sbtransaction.sbtxamount), 0) AS total_amount
FROM defog.broker.sbtransaction AS sbtransaction
JOIN defog.broker.sbcustomer AS sbcustomer
  ON LOWER(sbcustomer.sbcustcountry) = 'usa'
  AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
WHERE
  sbtransaction.sbtxdatetime < DATEADD(
    DAY,
    -(
      (
        DAYOFWEEK(TO_DATE(CURRENT_TIMESTAMP())) + 5
      ) % 7
    ),
    CAST(CURRENT_TIMESTAMP() AS DATE)
  )
  AND sbtransaction.sbtxdatetime >= DATEADD(
    DAY,
    -7,
    DATEADD(
      DAY,
      -(
        (
          DAYOFWEEK(TO_DATE(CURRENT_TIMESTAMP())) + 5
        ) % 7
      ),
      CAST(CURRENT_TIMESTAMP() AS DATE)
    )
  )
