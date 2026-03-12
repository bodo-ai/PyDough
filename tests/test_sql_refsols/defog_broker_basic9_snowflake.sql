WITH _u_0 AS (
  SELECT
    sbtxcustid AS _u_1
  FROM broker.sbtransaction
  GROUP BY
    1
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name
FROM broker.sbcustomer AS sbcustomer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbcustomer.sbcustid
WHERE
  _u_0._u_1 IS NULL
