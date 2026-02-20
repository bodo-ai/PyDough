SELECT
  CAST(DATE_SUB(
    CAST(sbTransaction.sbtxdatetime AS DATETIME),
    INTERVAL (
      (
        DAYOFWEEK(CAST(sbTransaction.sbtxdatetime AS DATETIME)) + 5
      ) % 7
    ) DAY
  ) AS DATE) AS week,
  COUNT(*) AS num_transactions,
  COALESCE(SUM((
    (
      DAYOFWEEK(sbTransaction.sbtxdatetime) + 5
    ) % 7
  ) IN (5, 6)), 0) AS weekend_transactions
FROM broker.sbTransaction AS sbTransaction
JOIN broker.sbTicker AS sbTicker
  ON sbTicker.sbtickerid = sbTransaction.sbtxtickerid
  AND sbTicker.sbtickertype = 'stock'
WHERE
  sbTransaction.sbtxdatetime < CAST(DATE_SUB(
    CURRENT_TIMESTAMP(),
    INTERVAL (
      (
        DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
      ) % 7
    ) DAY
  ) AS DATE)
  AND sbTransaction.sbtxdatetime >= DATE_SUB(
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
