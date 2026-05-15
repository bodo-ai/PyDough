WITH _s3 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM tpch.nation
), _s5 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.region
), _s14 AS (
  SELECT
    orders.o_custkey,
    part.p_type,
    _s5.r_name,
    COUNT(*) AS n_rows
  FROM tpch.part AS part
  JOIN tpch.lineitem AS lineitem
    ON CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) = 1992
    AND CAST(STRFTIME('%m', lineitem.l_shipdate) AS INTEGER) = 6
    AND lineitem.l_partkey = part.p_partkey
  JOIN tpch.supplier AS supplier
    ON lineitem.l_suppkey = supplier.s_suppkey
  JOIN _s3 AS _s3
    ON _s3.n_nationkey = supplier.s_nationkey
  JOIN _s5 AS _s5
    ON _s3.n_regionkey = _s5.r_regionkey
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1992
    AND lineitem.l_orderkey = orders.o_orderkey
  WHERE
    part.p_container LIKE 'SM%'
  GROUP BY
    1,
    2,
    3
), _t2 AS (
  SELECT
    _s13.r_name AS cust_region,
    _s14.r_name,
    SUM(_s14.n_rows) AS sum_n_rows
  FROM _s14 AS _s14
  JOIN tpch.customer AS customer
    ON _s14.o_custkey = customer.c_custkey
  JOIN _s3 AS _s11
    ON _s11.n_nationkey = customer.c_nationkey
  JOIN _s5 AS _s13
    ON _s11.n_regionkey = _s13.r_regionkey
  GROUP BY
    _s14.p_type,
    1,
    2
), _t1 AS (
  SELECT
    r_name,
    MAX(sum_n_rows) AS max_sum_n_rows,
    SUM(sum_n_rows) AS sum_sum_n_rows
  FROM _t2
  GROUP BY
    cust_region,
    1
)
SELECT
  r_name AS region,
  AVG(CAST((
    100.0 * max_sum_n_rows
  ) AS REAL) / sum_sum_n_rows) AS avgpct
FROM _t1
GROUP BY
  1
ORDER BY
  1
