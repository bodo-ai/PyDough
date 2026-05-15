WITH _s3 AS (
  SELECT
    coupons.merchant_id,
    COUNT(*) AS n_rows
  FROM ewallet.coupons AS coupons
  JOIN ewallet.merchants AS merchants
    ON DATEDIFF(MONTH, CAST(merchants.created_at AS DATETIME), CAST(coupons.created_at AS DATETIME)) = 0
    AND coupons.merchant_id = merchants.mid
  GROUP BY
    1
)
SELECT
  merchants.mid AS merchant_id,
  merchants.name AS merchant_name,
  COALESCE(_s3.n_rows, 0) AS coupons_per_merchant
FROM ewallet.merchants AS merchants
LEFT JOIN _s3 AS _s3
  ON _s3.merchant_id = merchants.mid
ORDER BY
  3 DESC NULLS LAST
LIMIT 1
