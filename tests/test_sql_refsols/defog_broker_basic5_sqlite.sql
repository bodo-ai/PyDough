WITH _u_0 AS (
  SELECT
    sbtransaction.sbtxcustid AS _u_1
  FROM main.sbtransaction AS sbtransaction
  WHERE
    sbtransaction.sbtxtype = 'buy'
  GROUP BY
    sbtransaction.sbtxcustid
)
SELECT
  sbcustomer.sbcustid AS _id
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbcustomer.sbcustid
WHERE
  NOT _u_0._u_1 IS NULL
