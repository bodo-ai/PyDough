WITH _t AS (
  SELECT
    o_custkey,
    o_orderdate,
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY CASE WHEN o_totalprice IS NULL THEN 1 ELSE 0 END DESC, o_totalprice DESC) AS _w
  FROM tpch.ORDERS
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
  CUSTOMER.c_name AS name
FROM tpch.CUSTOMER AS CUSTOMER
LEFT JOIN _s1 AS _s1
  ON CUSTOMER.c_custkey = _s1.o_custkey
WHERE
  CUSTOMER.c_nationkey = 6
ORDER BY
  CASE WHEN _s1.o_orderdate IS NULL THEN 1 ELSE 0 END,
  _s1.o_orderdate
LIMIT 5
