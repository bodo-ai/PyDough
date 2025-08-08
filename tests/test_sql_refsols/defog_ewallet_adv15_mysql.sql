WITH _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    coupons.merchant_id
  FROM main.coupons AS coupons
  JOIN main.merchants AS merchants
    ON (
      (
        YEAR(coupons.created_at) - YEAR(merchants.created_at)
      ) * 12 + (
        MONTH(coupons.created_at) - MONTH(merchants.created_at)
      )
    ) = 0
    AND coupons.merchant_id = merchants.mid
  GROUP BY
    coupons.merchant_id
)
SELECT
  merchants.mid AS merchant_id,
  merchants.name AS merchant_name,
  COALESCE(_s3.n_rows, 0) AS coupons_per_merchant
FROM main.merchants AS merchants
LEFT JOIN _s3 AS _s3
  ON _s3.merchant_id = merchants.mid
ORDER BY
  COALESCE(_s3.n_rows, 0) DESC
LIMIT 1
