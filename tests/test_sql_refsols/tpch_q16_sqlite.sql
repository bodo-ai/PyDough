WITH _t0 AS (
  SELECT
    COUNT(DISTINCT _s0.ps_suppkey) AS supplier_count,
    _s4.p_brand,
    _s4.p_size,
    _s4.p_type
  FROM tpch.partsupp AS _s0
  JOIN tpch.supplier AS _s1
    ON NOT _s1.s_comment LIKE '%Customer%Complaints%' AND _s0.ps_suppkey = _s1.s_suppkey
  JOIN tpch.part AS _s4
    ON NOT _s4.p_type LIKE 'MEDIUM POLISHED%%'
    AND _s0.ps_partkey = _s4.p_partkey
    AND _s4.p_brand <> 'BRAND#45'
    AND _s4.p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
  GROUP BY
    _s4.p_brand,
    _s4.p_size,
    _s4.p_type
)
SELECT
  p_brand AS P_BRAND,
  p_type AS P_TYPE,
  p_size AS P_SIZE,
  supplier_count AS SUPPLIER_COUNT
FROM _t0
ORDER BY
  supplier_count DESC,
  p_brand,
  p_type,
  p_size
LIMIT 10
