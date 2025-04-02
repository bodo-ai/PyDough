WITH "_t3_2" AS (
  SELECT
    COUNT() AS "agg_0",
    "coupons"."merchant_id" AS "merchant_id"
  FROM "main"."coupons" AS "coupons"
  LEFT JOIN "main"."merchants" AS "merchants"
    ON "coupons"."merchant_id" = "merchants"."mid"
  WHERE
    DATEDIFF("coupons"."created_at", "merchants"."created_at", MONTH) = 0
  GROUP BY
    "coupons"."merchant_id"
), "_t0_2" AS (
  SELECT
    "merchants"."mid" AS "merchant_id_6",
    COALESCE("_t3"."agg_0", 0) AS "coupons_per_merchant",
    "merchants"."name" AS "merchant_name",
    COALESCE("_t3"."agg_0", 0) AS "ordering_1"
  FROM "main"."merchants" AS "merchants"
  LEFT JOIN "_t3_2" AS "_t3"
    ON "_t3"."merchant_id" = "merchants"."mid"
  ORDER BY
    "ordering_1" DESC
  LIMIT 1
)
SELECT
  "_t0"."merchant_id_6" AS "merchant_id",
  "_t0"."merchant_name" AS "merchant_name",
  "_t0"."coupons_per_merchant" AS "coupons_per_merchant"
FROM "_t0_2" AS "_t0"
ORDER BY
  "_t0"."ordering_1" DESC
