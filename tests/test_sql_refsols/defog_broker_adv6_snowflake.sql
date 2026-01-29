WITH _s1 AS (
  SELECT
    sbtxcustid,
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbtxamount
  FROM broker.sbtransaction
  GROUP BY
    1
)
SELECT
  sbcustomer.sbcustname AS name,
  _s1.n_rows AS num_tx,
  COALESCE(_s1.sum_sbtxamount, 0) AS total_amount,
  RANK() OVER (ORDER BY COALESCE(_s1.sum_sbtxamount, 0) DESC) AS cust_rank
FROM broker.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
