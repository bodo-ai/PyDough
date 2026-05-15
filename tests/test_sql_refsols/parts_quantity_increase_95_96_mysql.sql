WITH _t4 AS (
  SELECT
    l_orderkey,
    l_partkey,
    l_quantity,
    l_shipmode
  FROM tpch.LINEITEM
  WHERE
    l_shipmode = 'RAIL'
), _s6 AS (
  SELECT
    _t4.l_partkey,
    ANY_VALUE(PART.p_name) AS anything_p_name,
    SUM(_t4.l_quantity) AS sum_l_quantity
  FROM tpch.PART AS PART
  JOIN _t4 AS _t4
    ON PART.p_partkey = _t4.l_partkey
  JOIN tpch.ORDERS AS ORDERS
    ON EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1995
    AND ORDERS.o_orderkey = _t4.l_orderkey
  WHERE
    PART.p_container LIKE 'SM%'
  GROUP BY
    1
)
SELECT
  ANY_VALUE(_s6.anything_p_name) COLLATE utf8mb4_bin AS name,
  COALESCE(ANY_VALUE(_s6.sum_l_quantity), 0) AS qty_95,
  COALESCE(SUM(_t6.l_quantity), 0) AS qty_96
FROM _s6 AS _s6
JOIN _t4 AS _t6
  ON _s6.l_partkey = _t6.l_partkey
JOIN tpch.ORDERS AS ORDERS
  ON EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1996
  AND ORDERS.o_orderkey = _t6.l_orderkey
GROUP BY
  _t6.l_partkey
ORDER BY
  COALESCE(SUM(_t6.l_quantity), 0) - COALESCE(ANY_VALUE(_s6.sum_l_quantity), 0) DESC,
  1
LIMIT 3
