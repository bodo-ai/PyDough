SELECT
  DATE_TRUNC(
    'DAY',
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) + 6
        ) % 7
      ) * -1,
      CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)
    )
  ) AS week,
  COUNT(*) AS num_transactions,
  COUNT_IF((
    (
      DAYOFWEEK(sbtransaction.sbtxdatetime) + 6
    ) % 7
  ) IN (5, 6)) AS weekend_transactions
FROM main.sbtransaction AS sbtransaction
JOIN main.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  AND sbticker.sbtickertype = 'stock'
WHERE
  sbtransaction.sbtxdatetime < DATE_TRUNC(
    'DAY',
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 6
        ) % 7
      ) * -1,
      CURRENT_TIMESTAMP()
    )
  )
  AND sbtransaction.sbtxdatetime >= DATEADD(
    WEEK,
    -8,
    DATE_TRUNC(
      'DAY',
      DATEADD(
        DAY,
        (
          (
            DAYOFWEEK(CURRENT_TIMESTAMP()) + 6
          ) % 7
        ) * -1,
        CURRENT_TIMESTAMP()
      )
    )
  )
GROUP BY
  1
