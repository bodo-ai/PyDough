SELECT
  PART.p_brand COLLATE utf8mb4_bin AS P_BRAND,
  PART.p_type COLLATE utf8mb4_bin AS P_TYPE,
  PART.p_size AS P_SIZE,
  COUNT(DISTINCT PARTSUPP.ps_suppkey) AS SUPPLIER_COUNT
FROM tpch.PARTSUPP AS PARTSUPP
JOIN tpch.SUPPLIER AS SUPPLIER
  ON NOT SUPPLIER.s_comment LIKE '%Customer%Complaints%'
  AND PARTSUPP.ps_suppkey = SUPPLIER.s_suppkey
JOIN tpch.PART AS PART
  ON NOT PART.p_type LIKE 'MEDIUM POLISHED%%'
  AND PART.p_brand <> 'BRAND#45'
  AND PART.p_partkey = PARTSUPP.ps_partkey
  AND PART.p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
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
