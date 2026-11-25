WITH _t AS (
  SELECT
    o_custkey,
    o_orderkey,
    ROW_NUMBER() OVER (
      PARTITION BY o_custkey
      ORDER BY CASE WHEN o_totalprice IS NULL THEN 1 ELSE 0 END DESC, o_totalprice DESC, CASE WHEN o_orderkey IS NULL THEN 1 ELSE 0 END, o_orderkey
    ) AS _w
  FROM tpch.ORDERS
), _s1 AS (
  SELECT
    o_custkey,
    o_orderkey
  FROM _t
  WHERE
    _w = 1
)
SELECT
  CUSTOMER.c_custkey AS `key`,
  _s1.o_orderkey AS best_order
FROM tpch.CUSTOMER AS CUSTOMER
LEFT JOIN _s1 AS _s1
  ON CUSTOMER.c_custkey = _s1.o_custkey
WHERE
  CUSTOMER.c_custkey <= 3
ORDER BY
  1
