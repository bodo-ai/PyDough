WITH _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    sbCustomer.sbcustid AS sbCustId
  FROM main.sbCustomer AS sbCustomer
  JOIN main.sbTransaction AS sbTransaction
    ON MONTH(sbCustomer.sbcustjoindate) = MONTH(sbTransaction.sbtxdatetime)
    AND YEAR(sbCustomer.sbcustjoindate) = YEAR(sbTransaction.sbtxdatetime)
    AND sbCustomer.sbcustid = sbTransaction.sbtxcustid
  GROUP BY
    sbCustomer.sbcustid
)
SELECT
  sbCustomer.sbcustid AS _id,
  sbCustomer.sbcustname AS name,
  COALESCE(_s3.n_rows, 0) AS num_transactions
FROM main.sbCustomer AS sbCustomer
LEFT JOIN _s3 AS _s3
  ON _s3.sbCustId = sbCustomer.sbcustid
ORDER BY
  num_transactions DESC
LIMIT 1
