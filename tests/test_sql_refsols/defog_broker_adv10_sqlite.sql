WITH _s1 AS (
  SELECT
    CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) AS month_sbtxdatetime,
    CAST(STRFTIME('%Y', sbtxdatetime) AS INTEGER) AS year_sbtxdatetime,
    sbtxcustid,
    COUNT(*) AS n_rows
  FROM main.sbtransaction
  GROUP BY
    1,
    2,
    3
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name,
  _s1.n_rows AS num_transactions
FROM main.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.month_sbtxdatetime = CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER)
  AND _s1.sbtxcustid = sbcustomer.sbcustid
  AND _s1.year_sbtxdatetime = CAST(STRFTIME('%Y', sbcustomer.sbcustjoindate) AS INTEGER)
ORDER BY
  3 DESC
LIMIT 1
