WITH _s0 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.region
), _s3 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM tpch.nation
), _s15 AS (
  SELECT
    _s13.r_name,
    supplier.s_suppkey
  FROM tpch.supplier AS supplier
  JOIN _s3 AS _s11
    ON _s11.n_nationkey = supplier.s_nationkey
  JOIN _s0 AS _s13
    ON _s11.n_regionkey = _s13.r_regionkey
  WHERE
    supplier.s_acctbal < 0
)
SELECT
  ANY_VALUE(_s0.r_name) AS supplier_region,
  ANY_VALUE(_s1.r_name) AS customer_region,
  COUNT(*) AS region_combinations
FROM _s0 AS _s0
CROSS JOIN _s0 AS _s1
JOIN _s3 AS _s3
  ON _s1.r_regionkey = _s3.n_regionkey
JOIN tpch.customer AS customer
  ON _s3.n_nationkey = customer.c_nationkey AND customer.c_mktsegment = 'AUTOMOBILE'
JOIN tpch.orders AS orders
  ON customer.c_custkey = orders.o_custkey AND orders.o_clerk = 'Clerk#000000007'
JOIN tpch.lineitem AS lineitem
  ON EXTRACT(MONTH FROM CAST(lineitem.l_shipdate AS DATETIME)) = 3
  AND EXTRACT(YEAR FROM CAST(lineitem.l_shipdate AS DATETIME)) = 1998
  AND lineitem.l_orderkey = orders.o_orderkey
JOIN _s15 AS _s15
  ON _s0.r_name = _s15.r_name AND _s15.s_suppkey = lineitem.l_suppkey
GROUP BY
  _s0.r_regionkey,
  _s1.r_regionkey
