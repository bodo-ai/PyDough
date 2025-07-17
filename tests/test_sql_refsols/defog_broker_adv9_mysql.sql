SELECT
  STR_TO_DATE(
    CONCAT(
      YEAR(CAST(sbTransaction.sbtxdatetime AS DATETIME)),
      ' ',
      WEEK(CAST(sbTransaction.sbtxdatetime AS DATETIME), 1),
      ' 1'
    ),
    '%Y %u %w'
  ) AS week,
  COUNT(*) AS num_transactions,
  COALESCE(SUM((
    (
      DAYOFWEEK(sbTransaction.sbtxdatetime) + 6
    ) % 7
  ) IN (5, 6)), 0) AS weekend_transactions
FROM main.sbTransaction AS sbTransaction
JOIN main.sbTicker AS sbTicker
  ON sbTicker.sbtickerid = sbTransaction.sbtxtickerid
  AND sbTicker.sbtickertype = 'stock'
WHERE
  sbTransaction.sbtxdatetime < STR_TO_DATE(
    CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', WEEK(CURRENT_TIMESTAMP(), 1), ' 1'),
    '%Y %u %w'
  )
  AND sbTransaction.sbtxdatetime >= DATE_ADD(
    STR_TO_DATE(
      CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', WEEK(CURRENT_TIMESTAMP(), 1), ' 1'),
      '%Y %u %w'
    ),
    INTERVAL '-8' WEEK
  )
GROUP BY
  STR_TO_DATE(
    CONCAT(
      YEAR(CAST(sbTransaction.sbtxdatetime AS DATETIME)),
      ' ',
      WEEK(CAST(sbTransaction.sbtxdatetime AS DATETIME), 1),
      ' 1'
    ),
    '%Y %u %w'
  )
