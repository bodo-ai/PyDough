WITH "_t0" AS (
  SELECT
    COUNT() AS "agg_0",
    SUM("wallet_transactions_daily"."amount") AS "agg_1",
    "wallet_transactions_daily"."sender_id" AS "sender_id"
  FROM "main"."wallet_transactions_daily" AS "wallet_transactions_daily"
  WHERE
    CAST((
      JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE("wallet_transactions_daily"."created_at", 'start of day'))
    ) AS INTEGER) <= 7
  GROUP BY
    "wallet_transactions_daily"."sender_id"
), "_t0_2" AS (
  SELECT
    SUM("_t0"."agg_0") AS "agg_0",
    SUM("_t0"."agg_1") AS "agg_1"
  FROM "_t0" AS "_t0"
  JOIN "main"."users" AS "users"
    ON "_t0"."sender_id" = "users"."uid" AND "users"."country" = 'US'
)
SELECT
  "_t0"."agg_0" AS "num_transactions",
  CASE WHEN "_t0"."agg_0" > 0 THEN COALESCE("_t0"."agg_1", 0) ELSE NULL END AS "total_amount"
FROM "_t0_2" AS "_t0"
