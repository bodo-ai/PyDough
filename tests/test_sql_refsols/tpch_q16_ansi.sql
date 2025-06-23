SELECT
  part.p_brand AS P_BRAND,
  part.p_type AS P_TYPE,
  part.p_size AS P_SIZE,
  COUNT(DISTINCT partsupp.ps_suppkey) AS SUPPLIER_COUNT
FROM tpch.partsupp AS partsupp
JOIN tpch.supplier AS supplier
  ON NOT supplier.s_comment LIKE '%Customer%Complaints%'
  AND partsupp.ps_suppkey = supplier.s_suppkey
JOIN tpch.part AS part
  ON NOT part.p_type LIKE 'MEDIUM POLISHED%%'
  AND part.p_brand <> 'BRAND#45'
  AND part.p_partkey = partsupp.ps_partkey
  AND part.p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
GROUP BY
  part.p_brand,
  part.p_size,
  part.p_type
ORDER BY
  supplier_count DESC,
  p_brand,
  p_type,
  p_size
LIMIT 10
