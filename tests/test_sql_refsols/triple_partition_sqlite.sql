WITH _s3 AS (
  SELECT
    n_nationkey AS key,
    n_regionkey AS region_key
  FROM tpch.nation
), _t3 AS (
  SELECT
    COUNT() AS n_instances,
    region_2.r_name AS cust_region,
    region.r_name AS supp_region
  FROM tpch.part AS part
  JOIN tpch.lineitem AS lineitem
    ON CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) = 1992
    AND CAST(STRFTIME('%m', lineitem.l_shipdate) AS INTEGER) = 6
    AND lineitem.l_partkey = part.p_partkey
  JOIN tpch.supplier AS supplier
    ON lineitem.l_suppkey = supplier.s_suppkey
  JOIN _s3 AS _s3
    ON _s3.key = supplier.s_nationkey
  JOIN tpch.region AS region
    ON _s3.region_key = region.r_regionkey
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1992
    AND lineitem.l_orderkey = orders.o_orderkey
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
  JOIN _s3 AS _s11
    ON _s11.key = customer.c_nationkey
  JOIN tpch.region AS region_2
    ON _s11.region_key = region_2.r_regionkey
  WHERE
    part.p_container LIKE 'SM%'
  GROUP BY
    region_2.r_name,
    part.p_type,
    region.r_name
), _t2 AS (
  SELECT
    MAX(n_instances) AS agg_0,
    SUM(n_instances) AS agg_1,
    supp_region
  FROM _t3
  GROUP BY
    cust_region,
    supp_region
), _t0 AS (
  SELECT
    AVG(CAST((
      100.0 * agg_0
    ) AS REAL) / COALESCE(agg_1, 0)) AS avg_percentage,
    supp_region
  FROM _t2
  GROUP BY
    supp_region
)
SELECT
  supp_region AS region,
  avg_percentage AS avgpct
FROM _t0
ORDER BY
  supp_region
