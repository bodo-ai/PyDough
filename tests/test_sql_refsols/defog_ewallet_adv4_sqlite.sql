WITH "_t0_2" AS (
  SELECT
    COUNT() AS "agg_0",
    SUM("wallet_transactions_daily"."amount") AS "agg_1"
  FROM "main"."wallet_transactions_daily" AS "wallet_transactions_daily"
  JOIN "main"."users" AS "users"
    ON "users"."country" = 'US'
    AND "users"."uid" = "wallet_transactions_daily"."sender_id"
  WHERE
    CAST((
      JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE("wallet_transactions_daily"."created_at", 'start of day'))
    ) AS INTEGER) <= 7
)
SELECT
  "_t0"."agg_0" AS "num_transactions",
  CASE WHEN "_t0"."agg_0" > 0 THEN COALESCE("_t0"."agg_1", 0) ELSE NULL END AS "total_amount"
FROM "_t0_2" AS "_t0"
