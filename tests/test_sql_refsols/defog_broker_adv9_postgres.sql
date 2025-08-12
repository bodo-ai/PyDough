SELECT
  DATE_TRUNC('WEEK', CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) AS week,
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
  sbtransaction.sbtxdatetime < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP)
  AND sbtransaction.sbtxdatetime >= DATE_TRUNC('WEEK', CURRENT_TIMESTAMP) + INTERVAL '8 WEEK'
GROUP BY
  DATE_TRUNC('WEEK', CAST(sbtransaction.sbtxdatetime AS TIMESTAMP))
