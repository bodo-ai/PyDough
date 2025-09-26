WITH _s1 AS (
  SELECT
    sbtxcustid,
    MIN(sbtxdatetime) AS min_sbtxdatetime
  FROM main.sbtransaction
  GROUP BY
    1
)
SELECT
  sbcustomer.sbcustid AS cust_id,
  CAST((
    (
      (
        CAST((
          JULIANDAY(DATE(_s1.min_sbtxdatetime, 'start of day')) - JULIANDAY(DATE(sbcustomer.sbcustjoindate, 'start of day'))
        ) AS INTEGER) * 24 + CAST(STRFTIME('%H', _s1.min_sbtxdatetime) AS INTEGER) - CAST(STRFTIME('%H', sbcustomer.sbcustjoindate) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%M', _s1.min_sbtxdatetime) AS INTEGER) - CAST(STRFTIME('%M', sbcustomer.sbcustjoindate) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%S', _s1.min_sbtxdatetime) AS INTEGER) - CAST(STRFTIME('%S', sbcustomer.sbcustjoindate) AS INTEGER)
  ) AS REAL) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM main.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
