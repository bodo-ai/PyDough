WITH _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    coupons.merchant_id
  FROM main.coupons AS coupons
  JOIN main.merchants AS merchants
    ON DATEDIFF(CAST(coupons.created_at AS DATETIME), CAST(merchants.created_at AS DATETIME), MONTH) = 0
    AND coupons.merchant_id = merchants.mid
  GROUP BY
    2
)
SELECT
  merchants.mid AS merchant_id,
  merchants.name AS merchant_name,
  COALESCE(_s3.n_rows, 0) AS coupons_per_merchant
FROM main.merchants AS merchants
LEFT JOIN _s3 AS _s3
  ON _s3.merchant_id = merchants.mid
ORDER BY
  3 DESC
LIMIT 1
