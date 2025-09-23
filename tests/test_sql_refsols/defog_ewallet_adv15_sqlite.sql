WITH _s3 AS (
  SELECT
    coupons.merchant_id,
    COUNT(*) AS n_rows
  FROM main.coupons AS coupons
  JOIN main.merchants AS merchants
    ON (
      (
        CAST(STRFTIME('%Y', coupons.created_at) AS INTEGER) - CAST(STRFTIME('%Y', merchants.created_at) AS INTEGER)
      ) * 12 + CAST(STRFTIME('%m', coupons.created_at) AS INTEGER) - CAST(STRFTIME('%m', merchants.created_at) AS INTEGER)
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
