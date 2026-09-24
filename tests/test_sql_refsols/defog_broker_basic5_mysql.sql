WITH _u_0 AS (
  SELECT
    sbTxCustId AS _u_1
  FROM broker.sbTransaction
  WHERE
    sbTxType = 'buy'
  GROUP BY
    1
)
SELECT
  sbCustomer.sbCustId AS _id
FROM broker.sbCustomer AS sbCustomer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbCustomer.sbCustId
WHERE
  NOT _u_0._u_1 IS NULL
