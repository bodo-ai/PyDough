SELECT
  DATE_TRUNC(
    'DAY',
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CAST(SBTRANSACTION.sbtxdatetime AS TIMESTAMP)) + 6
        ) % 7
      ) * -1,
      CAST(SBTRANSACTION.sbtxdatetime AS TIMESTAMP)
    )
  ) AS week,
  COUNT(*) AS num_transactions,
  COALESCE(
    COUNT_IF((
      (
        DAYOFWEEK(SBTRANSACTION.sbtxdatetime) + 6
      ) % 7
    ) IN (5, 6)),
    0
  ) AS weekend_transactions
FROM MAIN.SBTRANSACTION AS SBTRANSACTION
JOIN MAIN.SBTICKER AS SBTICKER
  ON SBTICKER.sbtickerid = SBTRANSACTION.sbtxtickerid
  AND SBTICKER.sbtickertype = 'stock'
WHERE
  SBTRANSACTION.sbtxdatetime < DATE_TRUNC(
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
  AND SBTRANSACTION.sbtxdatetime >= DATEADD(
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
