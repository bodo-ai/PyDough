WITH _s1 AS (
  SELECT
    EXTRACT(MONTH FROM CAST(sbtxdatetime AS DATETIME)) AS month_sbTxDateTime,
    COUNT(*) AS n_rows,
    EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)) AS year_sbTxDateTime,
    sbtxcustid AS sbTxCustId
  FROM main.sbTransaction
  GROUP BY
    1,
    3,
    4
)
SELECT
  sbCustomer.sbcustid AS _id,
  sbCustomer.sbcustname AS name,
  COALESCE(_s1.n_rows, 0) AS num_transactions
FROM main.sbCustomer AS sbCustomer
LEFT JOIN _s1 AS _s1
  ON _s1.month_sbTxDateTime = EXTRACT(MONTH FROM CAST(sbCustomer.sbcustjoindate AS DATETIME))
  AND _s1.sbTxCustId = sbCustomer.sbcustid
  AND _s1.year_sbTxDateTime = EXTRACT(YEAR FROM CAST(sbCustomer.sbcustjoindate AS DATETIME))
ORDER BY
  3 DESC
LIMIT 1
