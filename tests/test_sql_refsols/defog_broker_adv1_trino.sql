WITH _s1 AS (
  SELECT
    sbtxcustid,
    SUM(sbtxamount) AS sum_sbtxamount
  FROM mysql.broker.sbtransaction
  GROUP BY
    1
)
SELECT
  sbcustomer.sbcustname AS name,
  COALESCE(_s1.sum_sbtxamount, 0) AS total_amount
FROM postgres.main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
ORDER BY
  2 DESC
LIMIT 5
