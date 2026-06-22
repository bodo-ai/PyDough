SELECT
  DATEADD(
    DAY,
    -(
      (
        DAYOFWEEK(TO_DATE(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP))) + 5
      ) % 7
    ),
    CAST(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP) AS DATE)
  ) AS week,
  COUNT(*) AS num_transactions,
  COUNT_IF((
    (
      DAYOFWEEK(TO_DATE(sbtransaction.sbtxdatetime)) + 5
    ) % 7
  ) IN (5, 6)) AS weekend_transactions
FROM main.sbtransaction AS sbtransaction
JOIN main.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  AND sbticker.sbtickertype = 'stock'
WHERE
  sbtransaction.sbtxdatetime < DATEADD(
    DAY,
    -(
      (
        DAYOFWEEK(TO_DATE(CURRENT_TIMESTAMP())) + 5
      ) % 7
    ),
    CAST(CURRENT_TIMESTAMP() AS DATE)
  )
  AND sbtransaction.sbtxdatetime >= DATEADD(
    DAY,
    -56,
    DATEADD(
      DAY,
      -(
        (
          DAYOFWEEK(TO_DATE(CURRENT_TIMESTAMP())) + 5
        ) % 7
      ),
      CAST(CURRENT_TIMESTAMP() AS DATE)
    )
  )
GROUP BY
  1
