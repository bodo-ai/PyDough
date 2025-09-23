WITH _s3 AS (
  SELECT
    coupons.merchant_id,
    COUNT(*) AS n_rows
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
    1
)
SELECT
  merchants.mid AS merchant_id,
  merchants.name AS merchant_name,
  _s3.n_rows AS coupons_per_merchant
FROM main.merchants AS merchants
JOIN _s3 AS _s3
  ON _s3.merchant_id = merchants.mid
ORDER BY
  3 DESC
LIMIT 1
