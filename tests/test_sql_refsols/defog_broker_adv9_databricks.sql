SELECT
  DATE_ADD(
    CAST(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP) AS DATE),
    -(
      (
        DAYOFWEEK(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) + 5
      ) % 7
    )
  ) AS week,
  COUNT(*) AS num_transactions,
  COALESCE(
    COUNT_IF((
      (
        DAYOFWEEK(sbtransaction.sbtxdatetime) + 5
      ) % 7
    ) IN (5, 6)),
    0
  ) AS weekend_transactions
FROM defog.broker.sbtransaction AS sbtransaction
JOIN defog.broker.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  AND sbticker.sbtickertype = 'stock'
WHERE
  sbtransaction.sbtxdatetime < DATE_ADD(
    CAST(CURRENT_TIMESTAMP() AS DATE),
    -(
      (
        DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
      ) % 7
    )
  )
  AND sbtransaction.sbtxdatetime >= DATE_ADD(
    DATE_ADD(
      CAST(CURRENT_TIMESTAMP() AS DATE),
      -(
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      )
    ),
    -56
  )
GROUP BY
  1
