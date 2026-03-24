WITH _s3 AS (
  SELECT
    coupons.merchant_id,
    COUNT(*) AS n_rows
  FROM postgres.main.coupons AS coupons
  JOIN postgres.main.merchants AS merchants
    ON DATE_DIFF(
      'MONTH',
      CAST(DATE_TRUNC('MONTH', CAST(merchants.created_at AS TIMESTAMP)) AS TIMESTAMP),
      CAST(DATE_TRUNC('MONTH', CAST(coupons.created_at AS TIMESTAMP)) AS TIMESTAMP)
    ) = 0
    AND coupons.merchant_id = merchants.mid
  GROUP BY
    1
)
SELECT
  merchants.mid AS merchant_id,
  merchants.name AS merchant_name,
  COALESCE(_s3.n_rows, 0) AS coupons_per_merchant
FROM postgres.main.merchants AS merchants
LEFT JOIN _s3 AS _s3
  ON _s3.merchant_id = merchants.mid
ORDER BY
  3 DESC
LIMIT 1
