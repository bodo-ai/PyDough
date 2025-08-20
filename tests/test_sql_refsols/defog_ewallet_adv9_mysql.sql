SELECT
  STR_TO_DATE(
    CONCAT(YEAR(CAST(created_at AS DATETIME)), ' ', MONTH(CAST(created_at AS DATETIME)), ' 1'),
    '%Y %c %e'
  ) AS `year_month`,
  COUNT(DISTINCT sender_id) AS active_users
FROM main.wallet_transactions_daily
WHERE
  created_at < STR_TO_DATE(
    CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
    '%Y %c %e'
  )
  AND created_at >= DATE_ADD(
    STR_TO_DATE(
      CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
      '%Y %c %e'
    ),
    INTERVAL '-2' MONTH
  )
  AND sender_type = 0
GROUP BY
  1
