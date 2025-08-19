WITH _s1 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)) AS expr_1,
    EXTRACT(MONTH FROM CAST(sbtxdatetime AS DATETIME)) AS expr_2,
    COUNT(*) AS n_rows,
    sbtxcustid AS sbTxCustId
  FROM main.sbTransaction
  GROUP BY
    1,
    2,
    4
)
SELECT
  sbCustomer.sbcustid AS _id,
  sbCustomer.sbcustname AS name,
  COALESCE(_s1.n_rows, 0) AS num_transactions
FROM main.sbCustomer AS sbCustomer
LEFT JOIN _s1 AS _s1
  ON _s1.expr_1 = EXTRACT(YEAR FROM CAST(sbCustomer.sbcustjoindate AS DATETIME))
  AND _s1.expr_2 = EXTRACT(MONTH FROM CAST(sbCustomer.sbcustjoindate AS DATETIME))
  AND _s1.sbTxCustId = sbCustomer.sbcustid
ORDER BY
  3 DESC
LIMIT 1
