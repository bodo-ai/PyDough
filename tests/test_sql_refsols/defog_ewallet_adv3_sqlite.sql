WITH "_t1_2" AS (
  SELECT
    COUNT() AS "agg_0",
    "coupons"."merchant_id" AS "merchant_id"
  FROM "main"."coupons" AS "coupons"
  GROUP BY
    "coupons"."merchant_id"
)
SELECT
  "merchants"."name" AS "merchant_name",
  COALESCE("_t1"."agg_0", 0) AS "total_coupons"
FROM "main"."merchants" AS "merchants"
JOIN "_t1_2" AS "_t1"
  ON "_t1"."merchant_id" = "merchants"."mid"
WHERE
  "merchants"."status" = 'active'
  AND LOWER("merchants"."category") LIKE '%%retail%%'
