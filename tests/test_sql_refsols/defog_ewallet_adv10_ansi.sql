WITH "_t1_2" AS (
  SELECT
    COUNT() AS "agg_0",
    "wallet_transactions_daily"."sender_id" AS "sender_id"
  FROM "main"."wallet_transactions_daily" AS "wallet_transactions_daily"
  WHERE
    "wallet_transactions_daily"."sender_type" = 0
  GROUP BY
    "wallet_transactions_daily"."sender_id"
)
SELECT
  "users"."uid" AS "user_id",
  COALESCE("_t1"."agg_0", 0) AS "total_transactions"
FROM "main"."users" AS "users"
JOIN "_t1_2" AS "_t1"
  ON "_t1"."sender_id" = "users"."uid"
