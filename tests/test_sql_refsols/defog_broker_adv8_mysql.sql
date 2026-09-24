SELECT
  NULLIF(COUNT(*), 0) AS n_transactions,
  COALESCE(SUM(sbTransaction.sbTxAmount), 0) AS total_amount
FROM broker.sbTransaction AS sbTransaction
JOIN broker.sbCustomer AS sbCustomer
  ON LOWER(sbCustomer.sbCustCountry) = 'usa'
  AND sbCustomer.sbCustId = sbTransaction.sbTxCustId
WHERE
  sbTransaction.sbTxDateTime < CAST(DATE_SUB(
    CURRENT_TIMESTAMP(),
    INTERVAL (
      (
        DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
      ) % 7
    ) DAY
  ) AS DATE)
  AND sbTransaction.sbTxDateTime >= DATE_SUB(
    CAST(DATE_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      ) DAY
    ) AS DATE),
    INTERVAL '1' WEEK
  )
