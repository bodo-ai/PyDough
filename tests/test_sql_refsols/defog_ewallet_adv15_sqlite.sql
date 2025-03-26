WITH _table_alias_2 AS (
  SELECT
    merchants.mid AS mid,
    merchants.name AS name
  FROM main.merchants AS merchants
), _table_alias_0 AS (
  SELECT
    coupons.created_at AS created_at,
    coupons.merchant_id AS merchant_id
  FROM main.coupons AS coupons
), _table_alias_1 AS (
  SELECT
    merchants.created_at AS created_at,
    merchants.mid AS mid
  FROM main.merchants AS merchants
), _table_alias_3 AS (
  SELECT
    COUNT() AS agg_0,
    _table_alias_0.merchant_id AS merchant_id
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.merchant_id = _table_alias_1.mid
  WHERE
    (
      (
        CAST(STRFTIME('%Y', _table_alias_0.created_at) AS INTEGER) - CAST(STRFTIME('%Y', _table_alias_1.created_at) AS INTEGER)
      ) * 12 + CAST(STRFTIME('%m', _table_alias_0.created_at) AS INTEGER) - CAST(STRFTIME('%m', _table_alias_1.created_at) AS INTEGER)
    ) = 0
  GROUP BY
    _table_alias_0.merchant_id
), _t0 AS (
  SELECT
    _table_alias_2.mid AS merchant_id_6,
    COALESCE(_table_alias_3.agg_0, 0) AS coupons_per_merchant,
    _table_alias_2.name AS merchant_name,
    COALESCE(_table_alias_3.agg_0, 0) AS ordering_1
  FROM _table_alias_2 AS _table_alias_2
  LEFT JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.mid = _table_alias_3.merchant_id
  ORDER BY
    ordering_1 DESC
  LIMIT 1
)
SELECT
  _t0.merchant_id_6 AS merchant_id,
  _t0.merchant_name AS merchant_name,
  _t0.coupons_per_merchant AS coupons_per_merchant
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC
