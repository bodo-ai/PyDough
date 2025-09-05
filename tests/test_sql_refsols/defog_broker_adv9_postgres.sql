SELECT
  DATE_TRUNC(
    'DAY',
    CAST(sbtransaction.sbtxdatetime AS TIMESTAMP) - MAKE_INTERVAL(
      days => (
        EXTRACT(DOW FROM CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) + 6
      ) % 7
    )
  ) AS week,
  COUNT(*) AS num_transactions,
  COALESCE(
    SUM(
      CASE
        WHEN (
          (
            EXTRACT(DOW FROM CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) + 6
          ) % 7
        ) IN (5, 6)
        THEN 1
        ELSE 0
      END
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
    CURRENT_TIMESTAMP - MAKE_INTERVAL(days => (
      EXTRACT(DOW FROM CURRENT_TIMESTAMP) + 6
    ) % 7)
  )
  AND sbtransaction.sbtxdatetime >= DATE_TRUNC(
    'DAY',
    CURRENT_TIMESTAMP - MAKE_INTERVAL(days => (
      EXTRACT(DOW FROM CURRENT_TIMESTAMP) + 6
    ) % 7)
  ) - INTERVAL '8 WEEK'
GROUP BY
  1
