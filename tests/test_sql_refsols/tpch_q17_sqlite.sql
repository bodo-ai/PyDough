WITH _t AS (
  SELECT
    _s1.l_extendedprice AS extended_price,
    _s1.l_quantity AS quantity,
    AVG(_s1.l_quantity) OVER (PARTITION BY _s1.l_partkey) AS _w
  FROM tpch.part AS _s0
  JOIN tpch.lineitem AS _s1
    ON _s0.p_partkey = _s1.l_partkey
  WHERE
    _s0.p_brand = 'Brand#23' AND _s0.p_container = 'MED BOX'
), _t0 AS (
  SELECT
    SUM(extended_price) AS agg_0
  FROM _t
  WHERE
    quantity < (
      0.2 * _w
    )
)
SELECT
  CAST(COALESCE(agg_0, 0) AS REAL) / 7.0 AS AVG_YEARLY
FROM _t0
