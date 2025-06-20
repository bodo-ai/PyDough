WITH _t0 AS (
  SELECT
    SUM(_s0.l_extendedprice * (
      1 - _s0.l_discount
    )) AS agg_0
  FROM tpch.lineitem AS _s0
  JOIN tpch.part AS _s1
    ON _s0.l_partkey = _s1.p_partkey
  WHERE
    (
      (
        _s0.l_quantity <= 11
        AND _s0.l_quantity >= 1
        AND _s1.p_brand = 'Brand#12'
        AND _s1.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND _s1.p_size <= 5
        AND _s1.p_size >= 1
      )
      OR (
        _s0.l_quantity <= 20
        AND _s0.l_quantity >= 10
        AND _s1.p_brand = 'Brand#23'
        AND _s1.p_container IN ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG')
        AND _s1.p_size <= 10
        AND _s1.p_size >= 1
      )
      OR (
        _s0.l_quantity <= 30
        AND _s0.l_quantity >= 20
        AND _s1.p_brand = 'Brand#34'
        AND _s1.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND _s1.p_size <= 15
        AND _s1.p_size >= 1
      )
    )
    AND _s0.l_shipinstruct = 'DELIVER IN PERSON'
    AND _s0.l_shipmode IN ('AIR', 'AIR REG')
)
SELECT
  COALESCE(agg_0, 0) AS REVENUE
FROM _t0
