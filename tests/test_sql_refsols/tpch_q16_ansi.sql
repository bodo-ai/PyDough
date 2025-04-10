WITH _t1_2 AS (
  SELECT
    COUNT(DISTINCT partsupp.ps_suppkey) AS supplier_count,
    COUNT(DISTINCT partsupp.ps_suppkey) AS ordering_1,
    part.p_brand,
    part.p_brand AS ordering_2,
    part.p_size,
    part.p_size AS ordering_4,
    part.p_type,
    part.p_type AS ordering_3
  FROM tpch.part AS part
  JOIN tpch.partsupp AS partsupp
    ON part.p_partkey = partsupp.ps_partkey
  LEFT JOIN tpch.supplier AS supplier
    ON partsupp.ps_suppkey = supplier.s_suppkey
  WHERE
    NOT part.p_type LIKE 'MEDIUM POLISHED%%'
    AND NOT supplier.s_comment LIKE '%Customer%Complaints%'
    AND part.p_brand <> 'BRAND#45'
    AND part.p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
  GROUP BY
    part.p_brand,
    part.p_size,
    part.p_type
), _t0_2 AS (
  SELECT
    p_brand,
    p_size,
    p_type,
    supplier_count,
    ordering_1,
    ordering_2,
    ordering_3,
    ordering_4
  FROM _t1_2
  ORDER BY
    ordering_1 DESC,
    ordering_2,
    ordering_3,
    ordering_4
  LIMIT 10
)
SELECT
  p_brand AS P_BRAND,
  p_type AS P_TYPE,
  p_size AS P_SIZE,
  supplier_count AS SUPPLIER_COUNT
FROM _t0_2
ORDER BY
  ordering_1 DESC,
  ordering_2,
  ordering_3,
  ordering_4
