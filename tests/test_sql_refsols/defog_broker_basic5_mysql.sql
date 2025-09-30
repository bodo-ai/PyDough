WITH _u_0 AS (
  SELECT
    sbtxcustid AS _u_1
  FROM main.sbTransaction
  WHERE
    sbtxtype = 'buy'
  GROUP BY
    1
)
SELECT
  sbCustomer.sbcustid AS _id
FROM main.sbCustomer AS sbCustomer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbCustomer.sbcustid
WHERE
  NOT _u_0._u_1 IS NULL
