WITH _s1 AS (
  SELECT
    sbtxcustid
  FROM main.sbtransaction
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name
FROM main.sbcustomer AS sbcustomer
ANTI JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
