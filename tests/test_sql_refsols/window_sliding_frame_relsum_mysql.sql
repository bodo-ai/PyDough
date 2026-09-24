SELECT
  sbTxId AS transaction_id,
  SUM(sbTxShares) OVER (
    ORDER BY sbTxDateTime, sbTxId COLLATE utf8mb4_bin
    ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING
  ) AS w1,
  SUM(sbTxShares) OVER (
    PARTITION BY sbTxCustId
    ORDER BY sbTxDateTime, sbTxId COLLATE utf8mb4_bin
    ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING
  ) AS w2,
  SUM(sbTxShares) OVER (
    ORDER BY sbTxDateTime, sbTxId COLLATE utf8mb4_bin
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
  ) AS w3,
  SUM(sbTxShares) OVER (
    PARTITION BY sbTxCustId
    ORDER BY sbTxDateTime, sbTxId COLLATE utf8mb4_bin
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
  ) AS w4,
  SUM(sbTxShares) OVER (
    ORDER BY sbTxDateTime, sbTxId COLLATE utf8mb4_bin
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING
  ) AS w5,
  SUM(sbTxShares) OVER (
    PARTITION BY sbTxCustId
    ORDER BY sbTxDateTime, sbTxId COLLATE utf8mb4_bin
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING
  ) AS w6,
  SUM(sbTxShares) OVER (
    ORDER BY sbTxDateTime, sbTxId COLLATE utf8mb4_bin
    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
  ) AS w7,
  SUM(sbTxShares) OVER (
    PARTITION BY sbTxCustId
    ORDER BY sbTxDateTime, sbTxId COLLATE utf8mb4_bin
    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
  ) AS w8
FROM main.sbTransaction
ORDER BY
  sbTxDateTime
LIMIT 8
