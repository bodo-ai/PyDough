WITH _s1 AS (
  SELECT
    MIN(sbtxdatetime) AS agg_0,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  sbcustomer.sbcustid AS cust_id,
  CAST((
    (
      (
        CAST((
          JULIANDAY(DATE(_s1.agg_0, 'start of day')) - JULIANDAY(DATE(sbcustomer.sbcustjoindate, 'start of day'))
        ) AS INTEGER) * 24 + CAST(STRFTIME('%H', _s1.agg_0) AS INTEGER) - CAST(STRFTIME('%H', sbcustomer.sbcustjoindate) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%M', _s1.agg_0) AS INTEGER) - CAST(STRFTIME('%M', sbcustomer.sbcustjoindate) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%S', _s1.agg_0) AS INTEGER) - CAST(STRFTIME('%S', sbcustomer.sbcustjoindate) AS INTEGER)
  ) AS REAL) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM main.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.customer_id = sbcustomer.sbcustid
