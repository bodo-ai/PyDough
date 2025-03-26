WITH _table_alias_0 AS (
  SELECT
    part.p_partkey AS key
  FROM tpch.part AS part
  WHERE
    part.p_brand = 'Brand#23' AND part.p_container = 'MED BOX'
), _table_alias_1 AS (
  SELECT
    AVG(lineitem.l_quantity) AS agg_0,
    lineitem.l_partkey AS part_key
  FROM tpch.lineitem AS lineitem
  GROUP BY
    lineitem.l_partkey
), _table_alias_2 AS (
  SELECT
    _table_alias_1.agg_0 AS part_avg_quantity,
    _table_alias_0.key AS key
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.key = _table_alias_1.part_key
), _table_alias_3 AS (
  SELECT
    lineitem.l_extendedprice AS extended_price,
    lineitem.l_partkey AS part_key,
    lineitem.l_quantity AS quantity
  FROM tpch.lineitem AS lineitem
), _t0 AS (
  SELECT
    SUM(_table_alias_3.extended_price) AS agg_0
  FROM _table_alias_2 AS _table_alias_2
  JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.key = _table_alias_3.part_key
    AND _table_alias_3.quantity < (
      0.2 * _table_alias_2.part_avg_quantity
    )
)
SELECT
  CAST(COALESCE(_t0.agg_0, 0) AS REAL) / 7.0 AS AVG_YEARLY
FROM _t0 AS _t0
