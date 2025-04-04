WITH "_t1" AS (
  SELECT
    COUNT() AS "agg_0",
    "wallet_transactions_daily"."status" AS "status"
  FROM "main"."wallet_transactions_daily" AS "wallet_transactions_daily"
  GROUP BY
    "wallet_transactions_daily"."status"
)
SELECT
  "_t1"."status" AS "status",
  COALESCE("_t1"."agg_0", 0) AS "count"
FROM "_t1" AS "_t1"
ORDER BY
  "count" DESC
LIMIT 3
