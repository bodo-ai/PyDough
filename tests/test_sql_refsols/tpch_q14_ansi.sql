WITH _table_alias_0 AS (
  SELECT
    lineitem.l_discount AS discount,
    lineitem.l_extendedprice AS extended_price,
    lineitem.l_partkey AS part_key
  FROM tpch.lineitem AS lineitem
  WHERE
    lineitem.l_shipdate < CAST('1995-10-01' AS DATE)
    AND lineitem.l_shipdate >= CAST('1995-09-01' AS DATE)
), _table_alias_1 AS (
  SELECT
    part.p_partkey AS key,
    part.p_type AS part_type
  FROM tpch.part AS part
), _t0 AS (
  SELECT
    SUM(
      CASE
        WHEN _table_alias_1.part_type LIKE 'PROMO%'
        THEN _table_alias_0.extended_price * (
          1 - _table_alias_0.discount
        )
        ELSE 0
      END
    ) AS agg_0,
    SUM(_table_alias_0.extended_price * (
      1 - _table_alias_0.discount
    )) AS agg_1
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.part_key = _table_alias_1.key
)
SELECT
  (
    100.0 * COALESCE(_t0.agg_0, 0)
  ) / COALESCE(_t0.agg_1, 0) AS PROMO_REVENUE
FROM _t0 AS _t0
