WITH _s2 AS (
  SELECT
    l_partkey,
    l_suppkey,
    COUNT(*) AS n_rows,
    SUM(l_quantity) AS sum_l_quantity
  FROM tpch.LINEITEM
  WHERE
    EXTRACT(YEAR FROM CAST(l_shipdate AS DATETIME)) = 1994 AND l_tax = 0
  GROUP BY
    1,
    2
), _t AS (
  SELECT
    _s2.l_suppkey,
    _s2.n_rows,
    PART.p_name,
    _s2.sum_l_quantity,
    ROW_NUMBER() OVER (PARTITION BY _s2.l_suppkey ORDER BY COALESCE(_s2.sum_l_quantity, 0) DESC) AS _w
  FROM _s2 AS _s2
  JOIN tpch.PART AS PART
    ON PART.p_partkey = _s2.l_partkey
)
SELECT
  SUPPLIER.s_name COLLATE utf8mb4_bin AS supplier_name,
  _t.p_name AS part_name,
  COALESCE(_t.sum_l_quantity, 0) AS total_quantity,
  _t.n_rows AS n_shipments
FROM tpch.SUPPLIER AS SUPPLIER
JOIN tpch.NATION AS NATION
  ON NATION.n_name = 'FRANCE' AND NATION.n_nationkey = SUPPLIER.s_nationkey
JOIN _t AS _t
  ON SUPPLIER.s_suppkey = _t.l_suppkey AND _t._w = 1
ORDER BY
  3 DESC,
  1
LIMIT 3
