WITH _s2 AS (
  SELECT
    'x' AS k,
    COUNT(*) AS n_rows
  FROM tpch.orders
  WHERE
    o_totalprice > 100000
), _s3 AS (
  SELECT
    'x' AS k,
    COUNT(*) AS n_rows
  FROM tpch.orders AS orders
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_orderkey = orders.o_orderkey
  WHERE
    orders.o_totalprice > 100000
)
SELECT
  _s2.k,
  _s2.n_rows AS n_orders,
  _s3.n_rows AS n_lines
FROM _s2 AS _s2
JOIN _s3 AS _s3
  ON _s2.k = _s3.k
