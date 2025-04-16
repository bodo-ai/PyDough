WITH _s1 AS (
  SELECT
    SUM(sbtxamount) AS agg_0,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  sbcustomer.sbcustname AS name,
  COALESCE(_s1.agg_0, 0) AS total_amount
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.customer_id = sbcustomer.sbcustid
ORDER BY
  total_amount DESC
LIMIT 5
