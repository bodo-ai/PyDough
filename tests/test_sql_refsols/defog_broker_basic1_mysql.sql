WITH _s1 AS (
  SELECT
    sbtxcustid AS sbTxCustId,
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbTxAmount
  FROM main.sbTransaction
  WHERE
    sbtxdatetime >= CAST(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '30' DAY) AS DATE)
  GROUP BY
    1
)
SELECT
  sbCustomer.sbcustcountry AS country,
  SUM(_s1.n_rows) AS num_transactions,
  COALESCE(SUM(_s1.sum_sbTxAmount), 0) AS total_amount
FROM main.sbCustomer AS sbCustomer
JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbcustid
GROUP BY
  1
