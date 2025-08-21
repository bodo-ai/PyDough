WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbtxamount,
    sbtxcustid
  FROM main.sbtransaction
  GROUP BY
    3
)
SELECT
  sbcustomer.sbcustname AS name,
  _s1.n_rows AS num_tx,
  COALESCE(_s1.sum_sbtxamount, 0) AS total_amount,
  RANK() OVER (ORDER BY COALESCE(_s1.sum_sbtxamount, 0) DESC NULLS FIRST) AS cust_rank
FROM main.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
