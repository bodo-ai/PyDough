WITH "_t1" AS (
  SELECT
    COUNT("wallet_transactions_daily"."txid") AS "agg_0",
    SUM("wallet_transactions_daily"."amount") AS "agg_1",
    "wallet_transactions_daily"."coupon_id" AS "coupon_id"
  FROM "main"."wallet_transactions_daily" AS "wallet_transactions_daily"
  GROUP BY
    "wallet_transactions_daily"."coupon_id"
)
SELECT
  "coupons"."code" AS "coupon_code",
  COALESCE("_t1"."agg_0", 0) AS "redemption_count",
  COALESCE("_t1"."agg_1", 0) AS "total_discount"
FROM "main"."coupons" AS "coupons"
LEFT JOIN "_t1" AS "_t1"
  ON "_t1"."coupon_id" = "coupons"."cid"
ORDER BY
  "redemption_count" DESC
LIMIT 3
