WITH _table_alias_0 AS (
  SELECT
    merchants.mid AS mid,
    merchants.name AS name
  FROM main.merchants AS merchants
  WHERE
    LOWER(merchants.category) LIKE '%%retail%%' AND merchants.status = 'active'
), _table_alias_1 AS (
  SELECT
    COUNT() AS agg_0,
    coupons.merchant_id AS merchant_id
  FROM main.coupons AS coupons
  GROUP BY
    coupons.merchant_id
)
SELECT
  _table_alias_0.name AS merchant_name,
  COALESCE(_table_alias_1.agg_0, 0) AS total_coupons
FROM _table_alias_0 AS _table_alias_0
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0.mid = _table_alias_1.merchant_id
