WITH _t1 AS (
  SELECT
    lineitem.l_discount,
    lineitem.l_extendedprice,
    nation.n_name,
    nation.n_nationkey,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY nation.n_nationkey ORDER BY lineitem.l_extendedprice * (
            1 - lineitem.l_discount
          ) DESC) - 1.0
        ) - (
          CAST((
            COUNT(lineitem.l_extendedprice * (
              1 - lineitem.l_discount
            )) OVER (PARTITION BY nation.n_nationkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN lineitem.l_extendedprice * (
        1 - lineitem.l_discount
      )
      ELSE NULL
    END AS expr_7
  FROM tpch.nation AS nation
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1996
    AND CAST(STRFTIME('%m', orders.o_orderdate) AS INTEGER) = 1
    AND customer.c_custkey = orders.o_custkey
    AND orders.o_orderpriority = '1-URGENT'
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_orderkey = orders.o_orderkey
    AND lineitem.l_shipmode = 'TRUCK'
    AND lineitem.l_tax < 0.05
  JOIN tpch.supplier AS supplier
    ON lineitem.l_suppkey = supplier.s_suppkey
    AND nation.n_nationkey = supplier.s_nationkey
)
SELECT
  MAX(n_name) AS nation_name,
  AVG(l_extendedprice * (
    1 - l_discount
  )) AS mean_rev,
  AVG(expr_7) AS median_rev
FROM _t1
GROUP BY
  n_nationkey
ORDER BY
  1
