WITH _t1 AS (
  SELECT
    COUNT() AS agg_0,
    merchant_id
  FROM main.coupons
  GROUP BY
    merchant_id
)
SELECT
  merchants.name AS merchant_name,
  COALESCE(_t1.agg_0, 0) AS total_coupons
FROM main.merchants AS merchants
JOIN _t1 AS _t1
  ON _t1.merchant_id = merchants.mid
WHERE
  LOWER(merchants.category) LIKE '%%retail%%' AND merchants.status = 'active'
