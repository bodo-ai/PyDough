WITH _t2 AS (
  SELECT
    _s1.l_extendedprice AS extended_price
  FROM tpch.part AS _s0
  JOIN tpch.lineitem AS _s1
    ON _s0.p_partkey = _s1.l_partkey
  WHERE
    _s0.p_brand = 'Brand#23' AND _s0.p_container = 'MED BOX'
  QUALIFY
    _s1.l_quantity < (
      0.2 * AVG(_s1.l_quantity) OVER (PARTITION BY _s1.l_partkey)
    )
), _t0 AS (
  SELECT
    SUM(extended_price) AS agg_0
  FROM _t2
)
SELECT
  COALESCE(agg_0, 0) / 7.0 AS AVG_YEARLY
FROM _t0
