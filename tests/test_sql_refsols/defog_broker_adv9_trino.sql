SELECT
  DATE_TRUNC(
    'DAY',
    DATE_ADD(
      'DAY',
      (
        (
          (
            (
              DAY_OF_WEEK(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) % 7
            ) + 1
          ) + -1
        ) % 7
      ) * -1,
      CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)
    )
  ) AS week,
  COUNT(*) AS num_transactions,
  COALESCE(
    SUM(
      (
        (
          (
            DAY_OF_WEEK(sbtransaction.sbtxdatetime) % 7
          ) + 0
        ) % 7
      ) IN (5, 6)
    ),
    0
  ) AS weekend_transactions
FROM main.sbtransaction AS sbtransaction
JOIN main.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  AND sbticker.sbtickertype = 'stock'
WHERE
  sbtransaction.sbtxdatetime < DATE_TRUNC(
    'DAY',
    DATE_ADD(
      'DAY',
      (
        (
          (
            DAY_OF_WEEK(CURRENT_TIMESTAMP) % 7
          ) + 0
        ) % 7
      ) * -1,
      CURRENT_TIMESTAMP
    )
  )
  AND sbtransaction.sbtxdatetime >= DATE_ADD(
    'WEEK',
    -8,
    DATE_TRUNC(
      'DAY',
      DATE_ADD(
        'DAY',
        (
          (
            (
              DAY_OF_WEEK(CURRENT_TIMESTAMP) % 7
            ) + 0
          ) % 7
        ) * -1,
        CURRENT_TIMESTAMP
      )
    )
  )
GROUP BY
  1
