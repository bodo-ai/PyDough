WITH _t1 AS (
  SELECT
    AVG(l_quantity) AS agg_0,
    l_partkey AS part_key
  FROM tpch.lineitem
  GROUP BY
    l_partkey
), _t0_2 AS (
  SELECT
    SUM(lineitem.l_extendedprice) AS agg_0
  FROM tpch.part AS part
  LEFT JOIN _t1 AS _t1
    ON _t1.part_key = part.p_partkey
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = part.p_partkey
    AND lineitem.l_quantity < (
      0.2 * _t1.agg_0
    )
  WHERE
    part.p_brand = 'Brand#23' AND part.p_container = 'MED BOX'
)
SELECT
  CAST(COALESCE(_t0.agg_0, 0) AS REAL) / 7.0 AS AVG_YEARLY
FROM _t0_2 AS _t0
