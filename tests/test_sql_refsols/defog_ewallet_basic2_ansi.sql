WITH _table_alias_0 AS (
  SELECT
    merchants.mid AS mid,
    merchants.name AS name
  FROM main.merchants AS merchants
), _table_alias_1 AS (
  SELECT
    coupons.merchant_id AS merchant_id
  FROM main.coupons AS coupons
)
SELECT
  _table_alias_0.mid AS merchant_id,
  _table_alias_0.name AS merchant_name
FROM _table_alias_0 AS _table_alias_0
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0.mid = _table_alias_1.merchant_id
