WITH "_t1_2" AS (
  SELECT
    COUNT(DISTINCT "wallet_transactions_daily"."sender_id") AS "agg_1",
    SUM("wallet_transactions_daily"."amount") AS "agg_0",
    "users"."country" AS "country"
  FROM "main"."wallet_transactions_daily" AS "wallet_transactions_daily"
  JOIN "main"."users" AS "users"
    ON "users"."uid" = "wallet_transactions_daily"."sender_id"
  WHERE
    "wallet_transactions_daily"."sender_type" = 0
  GROUP BY
    "users"."country"
)
SELECT
  "_t1"."country" AS "country",
  "_t1"."agg_1" AS "user_count",
  COALESCE("_t1"."agg_0", 0) AS "total_amount"
FROM "_t1_2" AS "_t1"
ORDER BY
  "total_amount" DESC
LIMIT 5
