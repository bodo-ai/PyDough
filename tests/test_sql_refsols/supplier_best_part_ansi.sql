WITH _s2 AS (
  SELECT
    l_partkey,
    l_suppkey,
    COUNT(*) AS n_rows,
    SUM(l_quantity) AS sum_l_quantity
  FROM tpch.lineitem
  WHERE
    EXTRACT(YEAR FROM CAST(l_shipdate AS DATETIME)) = 1994 AND l_tax = 0
  GROUP BY
    1,
    2
), _t2 AS (
  SELECT
    _s2.l_suppkey,
    _s2.n_rows,
    part.p_name,
    _s2.sum_l_quantity
  FROM _s2 AS _s2
  JOIN tpch.part AS part
    ON _s2.l_partkey = part.p_partkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY l_suppkey ORDER BY COALESCE(_s2.sum_l_quantity, 0) DESC NULLS FIRST) = 1
)
SELECT
  supplier.s_name AS supplier_name,
  _t2.p_name AS part_name,
  COALESCE(_t2.sum_l_quantity, 0) AS total_quantity,
  _t2.n_rows AS n_shipments
FROM tpch.supplier AS supplier
JOIN tpch.nation AS nation
  ON nation.n_name = 'FRANCE' AND nation.n_nationkey = supplier.s_nationkey
JOIN _t2 AS _t2
  ON _t2.l_suppkey = supplier.s_suppkey
ORDER BY
  3 DESC,
  1
LIMIT 3
