SELECT
  AVG("wallet_user_balance_daily"."balance") AS "AMB"
FROM "main"."wallet_user_balance_daily" AS "wallet_user_balance_daily"
WHERE
  CAST((
    JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE("wallet_user_balance_daily"."updated_at", 'start of day'))
  ) AS INTEGER) <= 7
