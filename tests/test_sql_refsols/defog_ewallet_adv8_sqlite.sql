WITH "_t1_2" AS (
  SELECT
    SUM("wallet_transactions_daily"."amount") AS "agg_0",
    "wallet_transactions_daily"."receiver_id" AS "receiver_id"
  FROM "main"."wallet_transactions_daily" AS "wallet_transactions_daily"
  WHERE
    "wallet_transactions_daily"."receiver_type" = 1
    AND "wallet_transactions_daily"."status" = 'success'
  GROUP BY
    "wallet_transactions_daily"."receiver_id"
)
SELECT
  "merchants"."mid" AS "merchants_id",
  "merchants"."name" AS "merchants_name",
  "merchants"."category" AS "category",
  COALESCE("_t1"."agg_0", 0) AS "total_revenue",
  ROW_NUMBER() OVER (ORDER BY COALESCE("_t1"."agg_0", 0) DESC) AS "mrr"
FROM "main"."merchants" AS "merchants"
JOIN "_t1_2" AS "_t1"
  ON "_t1"."receiver_id" = "merchants"."mid"
