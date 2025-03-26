WITH _table_alias_0 AS (
  SELECT
    merchants.mid AS mid,
    merchants.name AS name
  FROM main.merchants AS merchants
), _table_alias_1 AS (
  SELECT
    coupons.merchant_id AS merchant_id
  FROM main.coupons AS coupons
), _u_0 AS (
  SELECT
    _table_alias_1.merchant_id AS _u_1
  FROM _table_alias_1 AS _table_alias_1
  GROUP BY
    _table_alias_1.merchant_id
)
SELECT
  _table_alias_0.mid AS merchant_id,
  _table_alias_0.name AS merchant_name
FROM _table_alias_0 AS _table_alias_0
LEFT JOIN _u_0 AS _u_0
  ON _table_alias_0.mid = _u_0._u_1
WHERE
  _u_0._u_1 IS NULL
