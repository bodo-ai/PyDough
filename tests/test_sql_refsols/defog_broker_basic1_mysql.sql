WITH _s2 AS (
  SELECT DISTINCT
    sbCustCountry
  FROM broker.sbCustomer
), _s1 AS (
  SELECT
    sbTxCustId,
    COUNT(*) AS n_rows,
    SUM(sbTxAmount) AS sum_sbTxAmount
  FROM broker.sbTransaction
  WHERE
    sbTxDateTime >= CAST(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '30' DAY) AS DATE)
  GROUP BY
    1
), _s3 AS (
  SELECT
    sbCustomer.sbCustCountry,
    SUM(_s1.n_rows) AS sum_n_rows,
    SUM(_s1.sum_sbTxAmount) AS sum_sum_sbTxAmount
  FROM broker.sbCustomer AS sbCustomer
  JOIN _s1 AS _s1
    ON _s1.sbTxCustId = sbCustomer.sbCustId
  GROUP BY
    1
)
SELECT
  _s2.sbCustCountry AS country,
  COALESCE(_s3.sum_n_rows, 0) AS num_transactions,
  COALESCE(_s3.sum_sum_sbTxAmount, 0) AS total_amount
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.sbCustCountry = _s3.sbCustCountry
