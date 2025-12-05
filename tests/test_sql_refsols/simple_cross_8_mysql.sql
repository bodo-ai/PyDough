WITH _s0 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.REGION
), _s3 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM tpch.NATION
), _s15 AS (
  SELECT
    _s13.r_name,
    SUPPLIER.s_suppkey
  FROM tpch.SUPPLIER AS SUPPLIER
  JOIN _s3 AS _s11
    ON SUPPLIER.s_nationkey = _s11.n_nationkey
  JOIN _s0 AS _s13
    ON _s11.n_regionkey = _s13.r_regionkey
  WHERE
    SUPPLIER.s_acctbal < 0
)
SELECT
  ANY_VALUE(_s0.r_name) AS supplier_region,
  ANY_VALUE(_s1.r_name) AS customer_region,
  COUNT(*) AS region_combinations
FROM _s0 AS _s0
CROSS JOIN _s0 AS _s1
JOIN _s3 AS _s3
  ON _s1.r_regionkey = _s3.n_regionkey
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_mktsegment = 'AUTOMOBILE' AND CUSTOMER.c_nationkey = _s3.n_nationkey
JOIN tpch.ORDERS AS ORDERS
  ON CUSTOMER.c_custkey = ORDERS.o_custkey AND ORDERS.o_clerk = 'Clerk#000000007'
JOIN tpch.LINEITEM AS LINEITEM
  ON EXTRACT(MONTH FROM CAST(LINEITEM.l_shipdate AS DATETIME)) = 3
  AND EXTRACT(YEAR FROM CAST(LINEITEM.l_shipdate AS DATETIME)) = 1998
  AND LINEITEM.l_orderkey = ORDERS.o_orderkey
JOIN _s15 AS _s15
  ON LINEITEM.l_suppkey = _s15.s_suppkey AND _s0.r_name = _s15.r_name
GROUP BY
  _s0.r_regionkey,
  _s1.r_regionkey
