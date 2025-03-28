WITH _table_alias_1 AS (
  SELECT
    COUNT() AS agg_0,
    coupons.merchant_id AS merchant_id
  FROM main.coupons AS coupons
  GROUP BY
    coupons.merchant_id
)
SELECT
  merchants.name AS merchant_name,
  COALESCE(_table_alias_1.agg_0, 0) AS total_coupons
FROM main.merchants AS merchants
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_1.merchant_id = merchants.mid
WHERE
  LOWER(merchants.category) LIKE '%%retail%%' AND merchants.status = 'active'
