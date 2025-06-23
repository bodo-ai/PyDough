WITH _s1 AS (
  SELECT
    sbtxcustid AS customer_id,
    SUM(sbtxamount) AS sum_sbtxamount
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  sbcustomer.sbcustname AS name,
  COALESCE(_s1.sum_sbtxamount, 0) AS total_amount
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.customer_id = sbcustomer.sbcustid
ORDER BY
  total_amount DESC
LIMIT 5
