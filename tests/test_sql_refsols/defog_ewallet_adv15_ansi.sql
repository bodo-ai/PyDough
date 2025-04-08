WITH _s3 AS (
  SELECT
    COUNT() AS agg_0,
    coupons.merchant_id
  FROM main.coupons AS coupons
  LEFT JOIN main.merchants AS merchants
    ON coupons.merchant_id = merchants.mid
  WHERE
    DATEDIFF(coupons.created_at, merchants.created_at, MONTH) = 0
  GROUP BY
    coupons.merchant_id
)
SELECT
  merchants.mid AS merchant_id,
  merchants.name AS merchant_name,
  COALESCE(_s3.agg_0, 0) AS coupons_per_merchant
FROM main.merchants AS merchants
LEFT JOIN _s3 AS _s3
  ON _s3.merchant_id = merchants.mid
ORDER BY
  coupons_per_merchant DESC
LIMIT 1
