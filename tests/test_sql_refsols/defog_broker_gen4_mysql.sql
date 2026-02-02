WITH _s1 AS (
  SELECT
    sbtxcustid AS sbTxCustId,
    COUNT(*) AS n_rows
  FROM broker.sbTransaction
  WHERE
    CAST(CAST(sbtxdatetime AS DATETIME) AS DATE) = CAST('2023-04-01' AS DATE)
    AND sbtxtype = 'sell'
  GROUP BY
    1
)
SELECT
  sbCustomer.sbcustid AS _id,
  sbCustomer.sbcustname AS name,
  COALESCE(_s1.n_rows, 0) AS num_tx
FROM broker.sbCustomer AS sbCustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbcustid
ORDER BY
  3 DESC
LIMIT 1
