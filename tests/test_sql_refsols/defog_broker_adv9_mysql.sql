SELECT
  CAST(DATE_SUB(
    CAST(sbtransaction.sbtxdatetime AS DATETIME),
    INTERVAL (
      (
        DAYOFWEEK(CAST(sbtransaction.sbtxdatetime AS DATETIME)) + 5
      ) % 7
    ) DAY
  ) AS DATE) AS week,
  COUNT(*) AS num_transactions,
  COALESCE(SUM((
    (
      DAYOFWEEK(sbtransaction.sbtxdatetime) + 5
    ) % 7
  ) IN (5, 6)), 0) AS weekend_transactions
FROM main.sbtransaction AS sbtransaction
JOIN main.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  AND sbticker.sbtickertype = 'stock'
WHERE
  sbtransaction.sbtxdatetime < CAST(DATE_SUB(
    CURRENT_TIMESTAMP(),
    INTERVAL (
      (
        DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
      ) % 7
    ) DAY
  ) AS DATE)
  AND sbtransaction.sbtxdatetime >= DATE_ADD(
    CAST(DATE_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      ) DAY
    ) AS DATE),
    INTERVAL '-8' WEEK
  )
GROUP BY
  1
