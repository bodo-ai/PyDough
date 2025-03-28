WITH _table_alias_1 AS (
  SELECT
    coupons.merchant_id AS merchant_id
  FROM main.coupons AS coupons
), _table_alias_0 AS (
  SELECT
    merchants.mid AS mid,
    merchants.name AS name
  FROM main.merchants AS merchants
)
SELECT
  _table_alias_0.mid AS merchant_id,
  _table_alias_0.name AS merchant_name
FROM _table_alias_0 AS _table_alias_0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _table_alias_1 AS _table_alias_1
    WHERE
      _table_alias_0.mid = _table_alias_1.merchant_id
  )
