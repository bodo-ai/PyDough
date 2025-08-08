SELECT
  DATE(
    DATE_SUB(
      CAST(sbTransaction.sbtxdatetime AS DATETIME),
      INTERVAL (
        (
          DAYOFWEEK(CAST(sbTransaction.sbtxdatetime AS DATETIME)) + 5
        ) % 7
      ) DAY
    )
  ) AS week,
  COUNT(*) AS num_transactions,
  COALESCE(SUM((
    (
      DAYOFWEEK(sbTransaction.sbtxdatetime) + 5
    ) % 7
  ) IN (5, 6)), 0) AS weekend_transactions
FROM main.sbTransaction AS sbTransaction
JOIN main.sbTicker AS sbTicker
  ON sbTicker.sbtickerid = sbTransaction.sbtxtickerid
  AND sbTicker.sbtickertype = 'stock'
WHERE
  sbTransaction.sbtxdatetime < DATE(
    DATE_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      ) DAY
    )
  )
  AND sbTransaction.sbtxdatetime >= DATE_ADD(
    DATE_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      ) DAY
    ),
    INTERVAL '-8' WEEK
  )
GROUP BY
  DATE(
    DATE_SUB(
      CAST(sbTransaction.sbtxdatetime AS DATETIME),
      INTERVAL (
        (
          DAYOFWEEK(CAST(sbTransaction.sbtxdatetime AS DATETIME)) + 5
        ) % 7
      ) DAY
    )
  )
