WITH _s1 AS (
  SELECT
    merchant_id,
    COUNT(*) AS n_rows
  FROM main.coupons
  GROUP BY
    1
)
SELECT
  merchants.name AS merchant_name,
  _s1.n_rows AS total_coupons
FROM main.merchants AS merchants
JOIN _s1 AS _s1
  ON _s1.merchant_id = merchants.mid
WHERE
  CONTAINS(LOWER(merchants.category), 'retail') AND merchants.status = 'active'
