WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbTxAmount,
    sbtxcustid AS sbTxCustId
  FROM main.sbTransaction
  WHERE
    sbtxdatetime >= DATE(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '-30' DAY))
  GROUP BY
    sbtxcustid
)
SELECT
  sbCustomer.sbcustcountry AS country,
  COALESCE(SUM(_s1.n_rows), 0) AS num_transactions,
  COALESCE(SUM(_s1.sum_sbTxAmount), 0) AS total_amount
FROM main.sbCustomer AS sbCustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbcustid
GROUP BY
  1
