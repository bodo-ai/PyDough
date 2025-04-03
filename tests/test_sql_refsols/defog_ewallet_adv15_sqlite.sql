WITH "_t3_2" AS (
  SELECT
    COUNT() AS "agg_0",
    "coupons"."merchant_id" AS "merchant_id"
  FROM "main"."coupons" AS "coupons"
  LEFT JOIN "main"."merchants" AS "merchants"
    ON "coupons"."merchant_id" = "merchants"."mid"
  WHERE
    (
      (
        CAST(STRFTIME('%Y', "coupons"."created_at") AS INTEGER) - CAST(STRFTIME('%Y', "merchants"."created_at") AS INTEGER)
      ) * 12 + CAST(STRFTIME('%m', "coupons"."created_at") AS INTEGER) - CAST(STRFTIME('%m', "merchants"."created_at") AS INTEGER)
    ) = 0
  GROUP BY
    "coupons"."merchant_id"
)
SELECT
  "merchants"."mid" AS "merchant_id",
  "merchants"."name" AS "merchant_name",
  COALESCE("_t3"."agg_0", 0) AS "coupons_per_merchant"
FROM "main"."merchants" AS "merchants"
LEFT JOIN "_t3_2" AS "_t3"
  ON "_t3"."merchant_id" = "merchants"."mid"
ORDER BY
  "coupons_per_merchant" DESC
LIMIT 1
