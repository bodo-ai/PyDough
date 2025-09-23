WITH _s1 AS (
  SELECT
    sbtxcustid,
    SUM(sbtxamount) AS sum_sbtxamount
  FROM main.sbtransaction
  GROUP BY
    1
)
SELECT
  sbcustomer.sbcustname AS name,
  COALESCE(_s1.sum_sbtxamount, 0) AS total_amount
FROM main.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
ORDER BY
  2 DESC
LIMIT 5
