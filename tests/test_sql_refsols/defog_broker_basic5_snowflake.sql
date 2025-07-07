WITH _u_0 AS (
  SELECT
    sbtxcustid AS _u_1
  FROM MAIN.SBTRANSACTION
  WHERE
    sbtxtype = 'buy'
  GROUP BY
    sbtxcustid
)
SELECT
  SBCUSTOMER.sbcustid AS _id
FROM MAIN.SBCUSTOMER AS SBCUSTOMER
LEFT JOIN _u_0 AS _u_0
  ON SBCUSTOMER.sbcustid = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
