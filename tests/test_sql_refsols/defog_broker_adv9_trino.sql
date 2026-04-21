SELECT
  DATE_TRUNC(
    'DAY',
    DATE_ADD(
      'DAY',
      (
        (
          DAY_OF_WEEK(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) - 1
        ) % 7
      ) * -1,
      CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)
    )
  ) AS week,
  COUNT(*) AS num_transactions,
  COUNT_IF((
    (
      DAY_OF_WEEK(sbtransaction.sbtxdatetime) - 1
    ) % 7
  ) IN (5, 6)) AS weekend_transactions
FROM mysql.broker.sbtransaction AS sbtransaction
JOIN mysql.broker.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  AND sbticker.sbtickertype = 'stock'
WHERE
  sbtransaction.sbtxdatetime < DATE_TRUNC(
    'DAY',
    DATE_ADD(
      'DAY',
      (
        (
          DAY_OF_WEEK(CURRENT_TIMESTAMP) - 1
        ) % 7
      ) * -1,
      CURRENT_TIMESTAMP
    )
  )
  AND sbtransaction.sbtxdatetime >= DATE_ADD(
    'DAY',
    -56,
    DATE_TRUNC(
      'DAY',
      DATE_ADD(
        'DAY',
        (
          (
            DAY_OF_WEEK(CURRENT_TIMESTAMP) - 1
          ) % 7
        ) * -1,
        CURRENT_TIMESTAMP
      )
    )
  )
GROUP BY
  1
