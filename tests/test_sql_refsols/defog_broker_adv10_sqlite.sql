WITH _s3 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    COUNT(*) AS n_rows
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON CAST(STRFTIME('%Y', sbcustomer.sbcustjoindate) AS INTEGER) = CAST(STRFTIME('%Y', sbtransaction.sbtxdatetime) AS INTEGER)
    AND CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER) = CAST(STRFTIME('%m', sbtransaction.sbtxdatetime) AS INTEGER)
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  GROUP BY
    sbcustomer.sbcustid
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name,
  COALESCE(_s3.n_rows, 0) AS num_transactions
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s3 AS _s3
  ON _s3._id = sbcustomer.sbcustid
ORDER BY
  num_transactions DESC
LIMIT 1
