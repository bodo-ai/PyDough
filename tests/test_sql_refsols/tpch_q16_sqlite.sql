WITH _t0 AS (
  SELECT
    COUNT(DISTINCT partsupp.ps_suppkey) AS supplier_count,
    part.p_brand,
    part.p_size,
    part.p_type,
    part.p_brand,
    part.p_size,
    part.p_type
  FROM tpch.part AS part
  JOIN tpch.partsupp AS partsupp
    ON part.p_partkey = partsupp.ps_partkey
  JOIN tpch.supplier AS supplier
    ON NOT supplier.s_comment LIKE '%Customer%Complaints%'
    AND partsupp.ps_suppkey = supplier.s_suppkey
  WHERE
    NOT part.p_type LIKE 'MEDIUM POLISHED%%'
    AND part.p_brand <> 'BRAND#45'
    AND part.p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
  GROUP BY
    part.p_brand,
    part.p_size,
    part.p_type
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
