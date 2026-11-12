WITH _t AS (
  SELECT
    o_custkey,
    o_orderdate,
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_totalprice DESC) AS _w
  FROM tpch.orders
  WHERE
    o_orderpriority = '1-URGENT'
), _s1 AS (
  SELECT
    o_custkey,
    o_orderdate
  FROM _t
  WHERE
    _w = 1
)
SELECT
  customer.c_name AS name
FROM tpch.customer AS customer
LEFT JOIN _s1 AS _s1
  ON _s1.o_custkey = customer.c_custkey
WHERE
  customer.c_nationkey = 6
ORDER BY
  COALESCE(_s1.o_orderdate, CAST('2000-01-01' AS DATE))
LIMIT 5
