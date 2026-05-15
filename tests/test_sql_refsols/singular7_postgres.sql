WITH _s3 AS (
  SELECT
    l_partkey,
    l_suppkey
  FROM tpch.lineitem
  WHERE
    EXTRACT(YEAR FROM CAST(l_shipdate AS TIMESTAMP)) = 1994
), _t4 AS (
  SELECT
    partsupp.ps_suppkey,
    MAX(part.p_name) AS anything_p_name,
    COUNT(_s3.l_suppkey) AS count_l_suppkey
  FROM tpch.partsupp AS partsupp
  JOIN tpch.part AS part
    ON part.p_brand = 'Brand#13' AND part.p_partkey = partsupp.ps_partkey
  LEFT JOIN _s3 AS _s3
    ON _s3.l_partkey = partsupp.ps_partkey AND _s3.l_suppkey = partsupp.ps_suppkey
  GROUP BY
    partsupp.ps_partkey,
    1
), _t AS (
  SELECT
    ps_suppkey,
    anything_p_name,
    count_l_suppkey,
    ROW_NUMBER() OVER (PARTITION BY ps_suppkey ORDER BY COALESCE(count_l_suppkey, 0) DESC, anything_p_name) AS _w
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
  supplier.s_name AS supplier_name,
  _s5.anything_p_name AS part_name,
  _s5.n_orders
FROM tpch.supplier AS supplier
LEFT JOIN _s5 AS _s5
  ON _s5.ps_suppkey = supplier.s_suppkey
WHERE
  supplier.s_nationkey = 20
ORDER BY
  3 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 5
