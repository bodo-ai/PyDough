WITH _s3 AS (
  SELECT
    MIN(sbtxdatetime) AS agg_0,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  _s0.sbcustid AS cust_id,
  CAST((
    (
      (
        CAST((
          JULIANDAY(DATE(_s3.agg_0, 'start of day')) - JULIANDAY(DATE(_s0.sbcustjoindate, 'start of day'))
        ) AS INTEGER) * 24 + CAST(STRFTIME('%H', _s3.agg_0) AS INTEGER) - CAST(STRFTIME('%H', _s0.sbcustjoindate) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%M', _s3.agg_0) AS INTEGER) - CAST(STRFTIME('%M', _s0.sbcustjoindate) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%S', _s3.agg_0) AS INTEGER) - CAST(STRFTIME('%S', _s0.sbcustjoindate) AS INTEGER)
  ) AS REAL) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM main.sbcustomer AS _s0
JOIN _s3 AS _s3
  ON _s0.sbcustid = _s3.customer_id
