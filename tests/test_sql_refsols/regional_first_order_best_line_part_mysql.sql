WITH _t AS (
  SELECT
    NATION.n_regionkey,
    ORDERS.o_orderkey,
    ROW_NUMBER() OVER (PARTITION BY NATION.n_regionkey ORDER BY CASE WHEN ORDERS.o_orderdate IS NULL THEN 1 ELSE 0 END, ORDERS.o_orderdate, CASE WHEN ORDERS.o_orderkey IS NULL THEN 1 ELSE 0 END, ORDERS.o_orderkey) AS _w
  FROM tpch.NATION AS NATION
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
  JOIN tpch.ORDERS AS ORDERS
    ON CUSTOMER.c_custkey = ORDERS.o_custkey
    AND EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1992
), _t_2 AS (
  SELECT
    LINEITEM.l_partkey,
    _t.n_regionkey,
    ROW_NUMBER() OVER (PARTITION BY _t.n_regionkey ORDER BY CASE WHEN LINEITEM.l_quantity IS NULL THEN 1 ELSE 0 END DESC, LINEITEM.l_quantity DESC, CASE WHEN LINEITEM.l_linenumber IS NULL THEN 1 ELSE 0 END, LINEITEM.l_linenumber) AS _w
  FROM _t AS _t
  JOIN tpch.LINEITEM AS LINEITEM
    ON EXTRACT(YEAR FROM CAST(LINEITEM.l_shipdate AS DATETIME)) = 1992
    AND LINEITEM.l_orderkey = _t.o_orderkey
  WHERE
    _t._w = 1
), _s9 AS (
  SELECT
    _t.n_regionkey,
    PART.p_name
  FROM _t_2 AS _t
  JOIN tpch.PART AS PART
    ON PART.p_partkey = _t.l_partkey
  WHERE
    _t._w = 1
)
SELECT
  REGION.r_name COLLATE utf8mb4_bin AS region_name,
  _s9.p_name AS part_name
FROM tpch.REGION AS REGION
LEFT JOIN _s9 AS _s9
  ON REGION.r_regionkey = _s9.n_regionkey
ORDER BY
  1
