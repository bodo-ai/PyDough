SELECT
  DATE_TRUNC('MONTH', CAST("wallet_transactions_daily"."created_at" AS TIMESTAMP)) AS "year_month",
  COUNT(DISTINCT "wallet_transactions_daily"."sender_id") AS "active_users"
FROM "main"."wallet_transactions_daily" AS "wallet_transactions_daily"
WHERE
  "wallet_transactions_daily"."created_at" < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
  AND "wallet_transactions_daily"."created_at" >= DATE_ADD(DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()), -2, 'MONTH')
  AND "wallet_transactions_daily"."sender_type" = 0
GROUP BY
  DATE_TRUNC('MONTH', CAST("wallet_transactions_daily"."created_at" AS TIMESTAMP))
