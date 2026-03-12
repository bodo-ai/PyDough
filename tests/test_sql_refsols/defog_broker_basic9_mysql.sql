WITH _u_0 AS (
  SELECT
    sbtxcustid AS _u_1
  FROM broker.sbTransaction
  GROUP BY
    1
)
SELECT
  sbCustomer.sbcustid AS _id,
  sbCustomer.sbcustname AS name
FROM broker.sbCustomer AS sbCustomer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbCustomer.sbcustid
WHERE
  _u_0._u_1 IS NULL
