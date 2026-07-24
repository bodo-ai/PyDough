SELECT
  CAST(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP) AS DATE) - CAST((
    (
      DAYOFWEEK(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) + 6
    ) % 7
  ) AS INT) AS week,
  COUNT(*) AS num_transactions,
  COALESCE(
    COUNT_IF((
      (
        DAYOFWEEK(sbtransaction.sbtxdatetime) + 6
      ) % 7
    ) IN (5, 6)),
    0
  ) AS weekend_transactions
FROM main.sbtransaction AS sbtransaction
JOIN main.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  AND sbticker.sbtickertype = 'stock'
WHERE
  sbtransaction.sbtxdatetime < (
    CAST(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS DATE) - CAST((
      (
        DAYOFWEEK(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)) + 6
      ) % 7
    ) AS INT)
  )
  AND sbtransaction.sbtxdatetime >= CAST(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS DATE) - CAST((
    (
      DAYOFWEEK(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)) + 6
    ) % 7
  ) AS INT) - 7 * INTERVAL '8' DAY
GROUP BY
  1
