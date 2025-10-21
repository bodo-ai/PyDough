WITH _s1 AS (
  SELECT
    sbtxcustid AS sbTxCustId,
    sbtxdatetime AS sbTxDateTime
  FROM main.sbTransaction
), _t0 AS (
  SELECT
    _s1.sbTxCustId,
    ANY_VALUE(sbCustomer.sbcustname) AS anything_sbCustName,
    COUNT(*) AS n_rows
  FROM main.sbCustomer AS sbCustomer
  LEFT JOIN _s1 AS _s1
    ON EXTRACT(MONTH FROM CAST(_s1.sbTxDateTime AS DATETIME)) = EXTRACT(MONTH FROM CAST(sbCustomer.sbcustjoindate AS DATETIME))
    AND EXTRACT(YEAR FROM CAST(_s1.sbTxDateTime AS DATETIME)) = EXTRACT(YEAR FROM CAST(sbCustomer.sbcustjoindate AS DATETIME))
    AND _s1.sbTxCustId = sbCustomer.sbcustid
  GROUP BY
    EXTRACT(MONTH FROM CAST(_s1.sbTxDateTime AS DATETIME)),
    EXTRACT(YEAR FROM CAST(_s1.sbTxDateTime AS DATETIME)),
    1
)
SELECT
  sbTxCustId AS _id,
  anything_sbCustName AS name,
  n_rows * CASE WHEN NOT sbTxCustId IS NULL THEN 1 ELSE 0 END AS num_transactions
FROM _t0
ORDER BY
  3 DESC
LIMIT 1
