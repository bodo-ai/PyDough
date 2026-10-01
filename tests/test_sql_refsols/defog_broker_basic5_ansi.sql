WITH _s1 AS (
  SELECT
    sbtxcustid
  FROM main.sbtransaction
  WHERE
    sbtxtype = 'buy'
)
SELECT
  sbcustomer.sbcustid AS _id
FROM main.sbcustomer AS sbcustomer
SEMI JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
