WITH _s3 AS (
  SELECT
    COUNT(*) AS total_coupons,
    merchant_id
  FROM main.coupons
  GROUP BY
    merchant_id
)
SELECT
  _s0.name AS merchant_name,
  _s3.total_coupons
FROM main.merchants AS _s0
JOIN _s3 AS _s3
  ON _s0.mid = _s3.merchant_id
WHERE
  LOWER(_s0.category) LIKE '%retail%' AND _s0.status = 'active'
