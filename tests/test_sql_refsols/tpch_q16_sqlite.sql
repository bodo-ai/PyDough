WITH _t1 AS (
  SELECT
    COUNT(DISTINCT partsupp.ps_suppkey) AS supplier_count,
    COUNT(DISTINCT partsupp.ps_suppkey) AS ordering_1,
    part.p_brand AS p_brand,
    part.p_brand AS ordering_2,
    part.p_size AS p_size,
    part.p_size AS ordering_4,
    part.p_type AS p_type,
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
), _t0 AS (
  SELECT
    _t1.p_brand AS p_brand,
    _t1.p_size AS p_size,
    _t1.p_type AS p_type,
    _t1.supplier_count AS supplier_count,
    _t1.ordering_1 AS ordering_1,
    _t1.ordering_2 AS ordering_2,
    _t1.ordering_3 AS ordering_3,
    _t1.ordering_4 AS ordering_4
  FROM _t1 AS _t1
  ORDER BY
    ordering_1 DESC,
    ordering_2,
    ordering_3,
    ordering_4
  LIMIT 10
)
SELECT
  _t0.p_brand AS P_BRAND,
  _t0.p_type AS P_TYPE,
  _t0.p_size AS P_SIZE,
  _t0.supplier_count AS SUPPLIER_COUNT
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC,
  _t0.ordering_2,
  _t0.ordering_3,
  _t0.ordering_4
