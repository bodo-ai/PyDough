WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    merchant_id
  FROM main.coupons
  GROUP BY
    2
)
SELECT
  merchants.name AS merchant_name,
  _s1.n_rows AS total_coupons
FROM main.merchants AS merchants
JOIN _s1 AS _s1
  ON _s1.merchant_id = merchants.mid
WHERE
  LOWER(merchants.category) LIKE '%retail%' AND merchants.status = 'active'
