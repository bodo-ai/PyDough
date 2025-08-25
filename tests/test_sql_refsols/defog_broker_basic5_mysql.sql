WITH _u_0 AS (
  SELECT
    sbtxcustid AS _u_1
  FROM main.sbtransaction
  WHERE
    sbtxtype = 'buy'
  GROUP BY
    1
)
SELECT
  sbcustomer.sbcustid AS _id
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbcustomer.sbcustid
WHERE
  NOT _u_0._u_1 IS NULL
