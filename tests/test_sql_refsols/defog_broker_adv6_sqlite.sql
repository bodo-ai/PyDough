WITH _s1 AS (
  SELECT
    sbtxcustid AS customer_id,
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbtxamount
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  sbcustomer.sbcustname AS name,
  _s1.n_rows AS num_tx,
  COALESCE(_s1.sum_sbtxamount, 0) AS total_amount,
  RANK() OVER (ORDER BY COALESCE(_s1.sum_sbtxamount, 0) DESC) AS cust_rank
FROM main.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.customer_id = sbcustomer.sbcustid
