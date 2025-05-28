WITH _s1 AS (
  SELECT
    AVG(l_quantity) AS agg_0,
    l_partkey AS part_key
  FROM tpch.lineitem
  GROUP BY
    l_partkey
), _t0 AS (
  SELECT
    SUM(lineitem.l_extendedprice) AS agg_0
  FROM tpch.part AS part
  LEFT JOIN _s1 AS _s1
    ON _s1.part_key = part.p_partkey
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = part.p_partkey
  WHERE
    lineitem.l_quantity < (
      0.2 * _s1.agg_0
    )
    AND part.p_brand = 'Brand#23'
    AND part.p_container = 'MED BOX'
)
SELECT
  CAST(COALESCE(agg_0, 0) AS REAL) / 7.0 AS AVG_YEARLY
FROM _t0
