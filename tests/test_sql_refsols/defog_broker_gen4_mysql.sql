WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    sbtxcustid AS sbTxCustId
  FROM main.sbTransaction
  WHERE
    DATE(CAST(sbtxdatetime AS DATETIME)) = CAST('2023-04-01' AS DATE)
    AND sbtxtype = 'sell'
  GROUP BY
    sbtxcustid
)
SELECT
  sbCustomer.sbcustid AS _id,
  sbCustomer.sbcustname AS name,
  COALESCE(_s1.n_rows, 0) AS num_tx
FROM main.sbCustomer AS sbCustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbcustid
ORDER BY
  num_tx DESC
LIMIT 1
