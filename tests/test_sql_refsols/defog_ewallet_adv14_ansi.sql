WITH "_t0" AS (
  SELECT
    COUNT() AS "agg_1",
    SUM("wallet_transactions_daily"."status" = 'success') AS "agg_0"
  FROM "main"."wallet_transactions_daily" AS "wallet_transactions_daily"
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), "wallet_transactions_daily"."created_at", MONTH) = 1
)
SELECT
  COALESCE("_t0"."agg_0", 0) / "_t0"."agg_1" AS "_expr0"
FROM "_t0" AS "_t0"
