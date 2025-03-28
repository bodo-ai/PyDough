WITH _table_alias_1 AS (
  SELECT
    AVG(lineitem.l_quantity) AS agg_0,
    lineitem.l_partkey AS part_key
  FROM tpch.lineitem AS lineitem
  GROUP BY
    lineitem.l_partkey
), _t0 AS (
  SELECT
    SUM(lineitem.l_extendedprice) AS agg_0
  FROM tpch.part AS part
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_1.part_key = part.p_partkey
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = part.p_partkey
    AND lineitem.l_quantity < (
      0.2 * _table_alias_1.agg_0
    )
  WHERE
    part.p_brand = 'Brand#23' AND part.p_container = 'MED BOX'
)
SELECT
  COALESCE(_t0.agg_0, 0) / 7.0 AS AVG_YEARLY
FROM _t0 AS _t0
