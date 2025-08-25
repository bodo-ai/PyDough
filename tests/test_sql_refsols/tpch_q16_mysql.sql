SELECT
  p_brand COLLATE utf8mb4_bin AS P_BRAND,
  p_type COLLATE utf8mb4_bin AS P_TYPE,
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
  1,
  2,
  3
ORDER BY
  4 DESC,
  1,
  2,
  3
LIMIT 10
