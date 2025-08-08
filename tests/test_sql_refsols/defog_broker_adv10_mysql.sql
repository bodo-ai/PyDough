WITH _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    sbCustomer.sbcustid AS sbCustId
  FROM main.sbCustomer AS sbCustomer
  JOIN main.sbTransaction AS sbTransaction
    ON EXTRACT(MONTH FROM CAST(sbCustomer.sbcustjoindate AS DATETIME)) = EXTRACT(MONTH FROM CAST(sbTransaction.sbtxdatetime AS DATETIME))
    AND EXTRACT(YEAR FROM CAST(sbCustomer.sbcustjoindate AS DATETIME)) = EXTRACT(YEAR FROM CAST(sbTransaction.sbtxdatetime AS DATETIME))
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
  COALESCE(_s3.n_rows, 0) DESC
LIMIT 1
