WITH _s1 AS (
  SELECT
    sbtxcustid AS sbTxCustId,
    sbtxdatetime AS sbTxDateTime
  FROM main.sbTransaction
)
SELECT
  _s1.sbTxCustId AS _id,
  ANY_VALUE(sbCustomer.sbcustname) AS name,
  COUNT(*) AS num_transactions
FROM main.sbCustomer AS sbCustomer
LEFT JOIN _s1 AS _s1
  ON EXTRACT(MONTH FROM CAST(_s1.sbTxDateTime AS DATETIME)) = EXTRACT(MONTH FROM CAST(sbCustomer.sbcustjoindate AS DATETIME))
  AND EXTRACT(YEAR FROM CAST(_s1.sbTxDateTime AS DATETIME)) = EXTRACT(YEAR FROM CAST(sbCustomer.sbcustjoindate AS DATETIME))
  AND _s1.sbTxCustId = sbCustomer.sbcustid
GROUP BY
  EXTRACT(MONTH FROM CAST(_s1.sbTxDateTime AS DATETIME)),
  EXTRACT(YEAR FROM CAST(_s1.sbTxDateTime AS DATETIME)),
  1
ORDER BY
  3 DESC
LIMIT 1
