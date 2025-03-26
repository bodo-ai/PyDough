WITH _table_alias_0 AS (
  SELECT
    lineitem.l_discount AS discount,
    lineitem.l_extendedprice AS extended_price,
    lineitem.l_partkey AS part_key,
    lineitem.l_quantity AS quantity
  FROM tpch.lineitem AS lineitem
  WHERE
    lineitem.l_shipinstruct = 'DELIVER IN PERSON'
    AND lineitem.l_shipmode IN ('AIR', 'AIR REG')
), _table_alias_1 AS (
  SELECT
    part.p_brand AS brand,
    part.p_container AS container,
    part.p_partkey AS key,
    part.p_size AS size
  FROM tpch.part AS part
  WHERE
    part.p_size >= 1
), _t0 AS (
  SELECT
    SUM(_table_alias_0.extended_price * (
      1 - _table_alias_0.discount
    )) AS agg_0
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON (
      (
        _table_alias_0.quantity <= 11
        AND _table_alias_0.quantity >= 1
        AND _table_alias_1.brand = 'Brand#12'
        AND _table_alias_1.container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND _table_alias_1.size <= 5
      )
      OR (
        _table_alias_0.quantity <= 20
        AND _table_alias_0.quantity >= 10
        AND _table_alias_1.brand = 'Brand#23'
        AND _table_alias_1.container IN ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG')
        AND _table_alias_1.size <= 10
      )
      OR (
        _table_alias_0.quantity <= 30
        AND _table_alias_0.quantity >= 20
        AND _table_alias_1.brand = 'Brand#34'
        AND _table_alias_1.container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND _table_alias_1.size <= 15
      )
    )
    AND _table_alias_0.part_key = _table_alias_1.key
  WHERE
    (
      _table_alias_0.quantity <= 11
      AND _table_alias_0.quantity >= 1
      AND _table_alias_1.brand = 'Brand#12'
      AND _table_alias_1.container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
      AND _table_alias_1.size <= 5
    )
    OR (
      _table_alias_0.quantity <= 20
      AND _table_alias_0.quantity >= 10
      AND _table_alias_1.brand = 'Brand#23'
      AND _table_alias_1.container IN ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG')
      AND _table_alias_1.size <= 10
    )
    OR (
      _table_alias_0.quantity <= 30
      AND _table_alias_0.quantity >= 20
      AND _table_alias_1.brand = 'Brand#34'
      AND _table_alias_1.container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      AND _table_alias_1.size <= 15
    )
)
SELECT
  COALESCE(_t0.agg_0, 0) AS REVENUE
FROM _t0 AS _t0
