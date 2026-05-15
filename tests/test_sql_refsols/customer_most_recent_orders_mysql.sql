WITH _t AS (
  SELECT
    o_custkey,
    o_totalprice,
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY CASE WHEN o_orderdate IS NULL THEN 1 ELSE 0 END DESC, o_orderdate DESC, CASE WHEN o_orderkey IS NULL THEN 1 ELSE 0 END, o_orderkey) AS _w
  FROM tpch.ORDERS
), _s1 AS (
  SELECT
    o_custkey,
    SUM(o_totalprice) AS sum_o_totalprice
  FROM _t
  WHERE
    _w <= 5
  GROUP BY
    1
)
SELECT
  CUSTOMER.c_name AS name,
  COALESCE(_s1.sum_o_totalprice, 0) AS total_recent_value
FROM tpch.CUSTOMER AS CUSTOMER
JOIN _s1 AS _s1
  ON CUSTOMER.c_custkey = _s1.o_custkey
ORDER BY
  2 DESC
LIMIT 3
