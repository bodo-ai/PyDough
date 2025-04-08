WITH _s1 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(sbtxamount) AS agg_0,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  sbcustomer.sbcustname AS name,
  COALESCE(_s1.agg_1, 0) AS num_tx,
  COALESCE(_s1.agg_0, 0) AS total_amount,
  RANK() OVER (ORDER BY COALESCE(_s1.agg_0, 0) DESC NULLS FIRST) AS cust_rank
FROM main.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.customer_id = sbcustomer.sbcustid
