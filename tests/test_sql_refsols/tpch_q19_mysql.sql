SELECT
  COALESCE(SUM(LINEITEM.l_extendedprice * (
    1 - LINEITEM.l_discount
  )), 0) AS REVENUE
FROM tpch.LINEITEM AS LINEITEM
JOIN tpch.PART AS PART
  ON (
    (
      LINEITEM.l_quantity <= 11
      AND LINEITEM.l_quantity >= 1
      AND PART.p_brand = 'Brand#12'
      AND PART.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
      AND PART.p_size <= 5
      AND PART.p_size >= 1
    )
    OR (
      LINEITEM.l_quantity <= 20
      AND LINEITEM.l_quantity >= 10
      AND PART.p_brand = 'Brand#23'
      AND PART.p_container IN ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG')
      AND PART.p_size <= 10
      AND PART.p_size >= 1
    )
    OR (
      LINEITEM.l_quantity <= 30
      AND LINEITEM.l_quantity >= 20
      AND PART.p_brand = 'Brand#34'
      AND PART.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      AND PART.p_size <= 15
      AND PART.p_size >= 1
    )
  )
  AND LINEITEM.l_partkey = PART.p_partkey
WHERE
  LINEITEM.l_shipinstruct = 'DELIVER IN PERSON'
  AND LINEITEM.l_shipmode IN ('AIR', 'AIR REG')
