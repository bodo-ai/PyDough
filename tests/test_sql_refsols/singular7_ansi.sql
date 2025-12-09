WITH _s3 AS (
  SELECT
    l_partkey,
    l_suppkey
  FROM tpch.lineitem
  WHERE
    EXTRACT(YEAR FROM CAST(l_shipdate AS DATETIME)) = 1994
), _t3 AS (
  SELECT
    partsupp.ps_suppkey,
    ANY_VALUE(part.p_name) AS anything_p_name,
    COUNT(_s3.l_suppkey) AS count_l_suppkey
  FROM tpch.partsupp AS partsupp
  JOIN tpch.part AS part
    ON part.p_brand = 'Brand#13' AND part.p_partkey = partsupp.ps_partkey
  LEFT JOIN _s3 AS _s3
    ON _s3.l_partkey = partsupp.ps_partkey AND _s3.l_suppkey = partsupp.ps_suppkey
  GROUP BY
    partsupp.ps_partkey,
    1
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY ps_suppkey ORDER BY COUNT(_s3.l_suppkey) DESC NULLS FIRST, ANY_VALUE(part.p_name) NULLS LAST) = 1
)
SELECT
  supplier.s_name AS supplier_name,
  _t3.anything_p_name AS part_name,
  COALESCE(_t3.count_l_suppkey, 0) AS n_orders
FROM tpch.supplier AS supplier
LEFT JOIN _t3 AS _t3
  ON _t3.ps_suppkey = supplier.s_suppkey
WHERE
  supplier.s_nationkey = 20
ORDER BY
  3 DESC,
  1
LIMIT 5
