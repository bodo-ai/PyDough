WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbTxAmount,
    sbtxcustid AS sbTxCustId
  FROM main.sbTransaction
  GROUP BY
    sbtxcustid
)
SELECT
  sbCustomer.sbcustname AS name,
  _s1.n_rows AS num_tx,
  COALESCE(_s1.sum_sbTxAmount, 0) AS total_amount,
  RANK() OVER (ORDER BY 0 DESC, COALESCE(_s1.sum_sbTxAmount, 0) DESC) AS cust_rank
FROM main.sbCustomer AS sbCustomer
JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbcustid
