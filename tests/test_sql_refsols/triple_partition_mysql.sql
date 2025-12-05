WITH _s3 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM tpch.NATION
), _s5 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.REGION
), _s14 AS (
  SELECT
    ORDERS.o_custkey,
    PART.p_type,
    _s5.r_name,
    COUNT(*) AS n_rows
  FROM tpch.PART AS PART
  JOIN tpch.LINEITEM AS LINEITEM
    ON EXTRACT(MONTH FROM CAST(LINEITEM.l_shipdate AS DATETIME)) = 6
    AND EXTRACT(YEAR FROM CAST(LINEITEM.l_shipdate AS DATETIME)) = 1992
    AND LINEITEM.l_partkey = PART.p_partkey
  JOIN tpch.SUPPLIER AS SUPPLIER
    ON LINEITEM.l_suppkey = SUPPLIER.s_suppkey
  JOIN _s3 AS _s3
    ON SUPPLIER.s_nationkey = _s3.n_nationkey
  JOIN _s5 AS _s5
    ON _s3.n_regionkey = _s5.r_regionkey
  JOIN tpch.ORDERS AS ORDERS
    ON EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1992
    AND LINEITEM.l_orderkey = ORDERS.o_orderkey
  WHERE
    PART.p_container LIKE 'SM%'
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
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_custkey = _s14.o_custkey
  JOIN _s3 AS _s11
    ON CUSTOMER.c_nationkey = _s11.n_nationkey
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
  r_name COLLATE utf8mb4_bin AS region,
  AVG((
    100.0 * max_sum_n_rows
  ) / sum_sum_n_rows) AS avgpct
FROM _t1
GROUP BY
  1
ORDER BY
  1
