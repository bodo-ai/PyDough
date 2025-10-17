WITH _s1 AS (
  SELECT
    sbtxcustid AS sbTxCustId,
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbTxAmount
  FROM main.sbTransaction
  GROUP BY
    1
)
SELECT
  sbCustomer.sbcustname AS name,
  _s1.n_rows AS num_tx,
  COALESCE(_s1.sum_sbTxAmount, 0) AS total_amount,
  RANK() OVER (ORDER BY COALESCE(_s1.sum_sbTxAmount, 0) DESC) AS cust_rank
FROM main.sbCustomer AS sbCustomer
JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbcustid
