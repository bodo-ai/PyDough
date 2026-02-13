WITH _s3 AS (
  SELECT
    l_partkey,
    l_suppkey
  FROM tpch.LINEITEM
  WHERE
    EXTRACT(YEAR FROM CAST(l_shipdate AS DATETIME)) = 1994
), _t4 AS (
  SELECT
    PARTSUPP.ps_suppkey,
    ANY_VALUE(PART.p_name) AS anything_p_name,
    COUNT(_s3.l_suppkey) AS count_l_suppkey
  FROM tpch.PARTSUPP AS PARTSUPP
  JOIN tpch.PART AS PART
    ON PART.p_brand = 'Brand#13' AND PART.p_partkey = PARTSUPP.ps_partkey
  LEFT JOIN _s3 AS _s3
    ON PARTSUPP.ps_partkey = _s3.l_partkey AND PARTSUPP.ps_suppkey = _s3.l_suppkey
  GROUP BY
    PARTSUPP.ps_partkey,
    1
), _t AS (
  SELECT
    ps_suppkey,
    anything_p_name,
    count_l_suppkey,
    ROW_NUMBER() OVER (PARTITION BY ps_suppkey ORDER BY COALESCE(count_l_suppkey, 0) DESC, CASE WHEN anything_p_name COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, anything_p_name COLLATE utf8mb4_bin) AS _w
  FROM _t4
), _s5 AS (
  SELECT
    COALESCE(count_l_suppkey, 0) AS n_orders,
    anything_p_name,
    ps_suppkey
  FROM _t
  WHERE
    _w = 1
)
SELECT
  SUPPLIER.s_name COLLATE utf8mb4_bin AS supplier_name,
  _s5.anything_p_name AS part_name,
  _s5.n_orders
FROM tpch.SUPPLIER AS SUPPLIER
LEFT JOIN _s5 AS _s5
  ON SUPPLIER.s_suppkey = _s5.ps_suppkey
WHERE
  SUPPLIER.s_nationkey = 20
ORDER BY
  3 DESC,
  1
LIMIT 5
