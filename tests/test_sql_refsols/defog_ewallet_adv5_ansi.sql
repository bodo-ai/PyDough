SELECT
  AVG("wallet_user_balance_daily"."balance") AS "AMB"
FROM "main"."wallet_user_balance_daily" AS "wallet_user_balance_daily"
WHERE
  DATEDIFF(CURRENT_TIMESTAMP(), "wallet_user_balance_daily"."updated_at", DAY) <= 7
