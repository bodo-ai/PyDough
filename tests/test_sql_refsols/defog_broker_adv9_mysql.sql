SELECT
  CAST(DATE_SUB(
    CAST(sbTransaction.sbTxDateTime AS DATETIME),
    INTERVAL (
      (
        DAYOFWEEK(CAST(sbTransaction.sbTxDateTime AS DATETIME)) + 5
      ) % 7
    ) DAY
  ) AS DATE) AS week,
  COUNT(*) AS num_transactions,
  COALESCE(SUM((
    (
      DAYOFWEEK(sbTransaction.sbTxDateTime) + 5
    ) % 7
  ) IN (5, 6)), 0) AS weekend_transactions
FROM broker.sbTransaction AS sbTransaction
JOIN broker.sbTicker AS sbTicker
  ON sbTicker.sbTickerId = sbTransaction.sbTxTickerId
  AND sbTicker.sbTickerType = 'stock'
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
    INTERVAL '8' WEEK
  )
GROUP BY
  1
