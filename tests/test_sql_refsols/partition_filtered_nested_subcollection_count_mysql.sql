WITH _s2 AS (
  SELECT
    'x' AS k,
    COUNT(*) AS n_rows
  FROM tpch.ORDERS
  WHERE
    o_totalprice > 100000
), _s3 AS (
  SELECT
    'x' AS k,
    COUNT(*) AS n_rows
  FROM tpch.ORDERS AS ORDERS
  JOIN tpch.LINEITEM AS LINEITEM
    ON LINEITEM.l_orderkey = ORDERS.o_orderkey
  WHERE
    ORDERS.o_totalprice > 100000
)
SELECT
  _s2.k,
  _s2.n_rows AS n_orders,
  _s3.n_rows AS n_lines
FROM _s2 AS _s2
JOIN _s3 AS _s3
  ON _s2.k = _s3.k
