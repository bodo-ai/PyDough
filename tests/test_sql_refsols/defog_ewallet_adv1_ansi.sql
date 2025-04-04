WITH "_t0_2" AS (
  SELECT
    COUNT(DISTINCT "wallet_transactions_daily"."coupon_id") AS "agg_0",
    COUNT(DISTINCT "wallet_transactions_daily"."txid") AS "agg_1",
    "wallet_transactions_daily"."receiver_id" AS "receiver_id"
  FROM "main"."wallet_transactions_daily" AS "wallet_transactions_daily"
  WHERE
    "wallet_transactions_daily"."status" = 'success'
  GROUP BY
    "wallet_transactions_daily"."receiver_id"
)
SELECT
  "merchants"."name" AS "name",
  (
    "_t0"."agg_0" * 1.0
  ) / "_t0"."agg_1" AS "CPUR"
FROM "main"."merchants" AS "merchants"
JOIN "_t0_2" AS "_t0"
  ON "_t0"."receiver_id" = "merchants"."mid"
