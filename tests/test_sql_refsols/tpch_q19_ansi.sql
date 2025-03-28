WITH _t0 AS (
  SELECT
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS agg_0
  FROM tpch.lineitem AS lineitem
  JOIN tpch.part AS part
    ON (
      (
        lineitem.l_quantity <= 11
        AND lineitem.l_quantity >= 1
        AND part.p_brand = 'Brand#12'
        AND part.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND part.p_size <= 5
      )
      OR (
        lineitem.l_quantity <= 20
        AND lineitem.l_quantity >= 10
        AND part.p_brand = 'Brand#23'
        AND part.p_container IN ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG')
        AND part.p_size <= 10
      )
      OR (
        lineitem.l_quantity <= 30
        AND lineitem.l_quantity >= 20
        AND part.p_brand = 'Brand#34'
        AND part.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND part.p_size <= 15
      )
    )
    AND lineitem.l_partkey = part.p_partkey
    AND part.p_size >= 1
  WHERE
    (
      (
        lineitem.l_quantity <= 11
        AND lineitem.l_quantity >= 1
        AND part.p_brand = 'Brand#12'
        AND part.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND part.p_size <= 5
      )
      OR (
        lineitem.l_quantity <= 20
        AND lineitem.l_quantity >= 10
        AND part.p_brand = 'Brand#23'
        AND part.p_container IN ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG')
        AND part.p_size <= 10
      )
      OR (
        lineitem.l_quantity <= 30
        AND lineitem.l_quantity >= 20
        AND part.p_brand = 'Brand#34'
        AND part.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND part.p_size <= 15
      )
    )
    AND lineitem.l_shipinstruct = 'DELIVER IN PERSON'
    AND lineitem.l_shipmode IN ('AIR', 'AIR REG')
)
SELECT
  COALESCE(_t0.agg_0, 0) AS REVENUE
FROM _t0 AS _t0
