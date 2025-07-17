WITH _s7 AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS sum_value,
    l_orderkey,
    l_suppkey
  FROM tpch.LINEITEM
  GROUP BY
    l_orderkey,
    l_suppkey
), _s10 AS (
  SELECT
    ANY_VALUE(NATION.n_name) AS anything_n_name,
    SUM(_s7.sum_value) AS sum_sum_value,
    _s7.l_suppkey,
    NATION.n_name,
    NATION.n_nationkey
  FROM tpch.NATION AS NATION
  JOIN tpch.REGION AS REGION
    ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'ASIA'
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
  JOIN tpch.ORDERS AS ORDERS
    ON CUSTOMER.c_custkey = ORDERS.o_custkey
    AND ORDERS.o_orderdate < CAST('1995-01-01' AS DATE)
    AND ORDERS.o_orderdate >= CAST('1994-01-01' AS DATE)
  JOIN _s7 AS _s7
    ON ORDERS.o_orderkey = _s7.l_orderkey
  GROUP BY
    _s7.l_suppkey,
    NATION.n_name,
    NATION.n_nationkey
), _s11 AS (
  SELECT
    NATION.n_name,
    SUPPLIER.s_suppkey
  FROM tpch.SUPPLIER AS SUPPLIER
  JOIN tpch.NATION AS NATION
    ON NATION.n_nationkey = SUPPLIER.s_nationkey
)
SELECT
  ANY_VALUE(_s10.anything_n_name) AS N_NAME,
  COALESCE(SUM(_s10.sum_sum_value), 0) AS REVENUE
FROM _s10 AS _s10
JOIN _s11 AS _s11
  ON _s10.l_suppkey = _s11.s_suppkey AND _s10.n_name = _s11.n_name
GROUP BY
  _s10.n_nationkey
ORDER BY
  REVENUE DESC
