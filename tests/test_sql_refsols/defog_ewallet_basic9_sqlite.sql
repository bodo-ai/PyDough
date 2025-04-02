WITH "_t2" AS (
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
), "_t0_2" AS (
  SELECT
    "_t2"."country" AS "country",
    COALESCE("_t2"."agg_0", 0) AS "ordering_2",
    COALESCE("_t2"."agg_0", 0) AS "total_amount",
    "_t2"."agg_1" AS "user_count"
  FROM "_t2" AS "_t2"
  ORDER BY
    "ordering_2" DESC
  LIMIT 5
)
SELECT
  "_t0"."country" AS "country",
  "_t0"."user_count" AS "user_count",
  "_t0"."total_amount" AS "total_amount"
FROM "_t0_2" AS "_t0"
ORDER BY
  "_t0"."ordering_2" DESC
