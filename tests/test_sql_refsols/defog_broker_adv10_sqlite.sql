WITH _s1 AS (
  SELECT
    CAST(STRFTIME('%Y', sbtxdatetime) AS INTEGER) AS expr_1,
    CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) AS expr_2,
    COUNT(*) AS n_rows,
    sbtxcustid
  FROM main.sbtransaction
  GROUP BY
    CAST(STRFTIME('%Y', sbtxdatetime) AS INTEGER),
    CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER),
    sbtxcustid
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name,
  COALESCE(_s1.n_rows, 0) AS num_transactions
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.expr_1 = CAST(STRFTIME('%Y', sbcustomer.sbcustjoindate) AS INTEGER)
  AND _s1.expr_2 = CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER)
  AND _s1.sbtxcustid = sbcustomer.sbcustid
ORDER BY
  COALESCE(_s1.n_rows, 0) DESC
LIMIT 1
