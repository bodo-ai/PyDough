WITH _s5 AS (
  SELECT
    CAST((
      SUM(CAST(customer.c_acctbal * orders.o_totalprice AS REAL) / 1000000.0) - CAST(SUM(customer.c_acctbal) * SUM(CAST(orders.o_totalprice AS REAL) / 1000000.0) AS REAL) / SUM(
        CASE
          WHEN NOT CAST(orders.o_totalprice AS REAL) / 1000000.0 IS NULL
          AND NOT customer.c_acctbal IS NULL
          THEN 1
        END
      )
    ) AS REAL) / SUM(
      CASE
        WHEN NOT CAST(orders.o_totalprice AS REAL) / 1000000.0 IS NULL
        AND NOT customer.c_acctbal IS NULL
        THEN 1
      END
    ) AS agg_0,
    nation.n_regionkey
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_mktsegment = 'BUILDING' AND customer.c_nationkey = nation.n_nationkey
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1998
    AND customer.c_custkey = orders.o_custkey
    AND orders.o_orderpriority = '2-HIGH'
  GROUP BY
    nation.n_regionkey
)
SELECT
  region.r_name AS region_name,
  ROUND(_s5.agg_0, 3) AS cvp_ab_otp
FROM tpch.region AS region
LEFT JOIN _s5 AS _s5
  ON _s5.n_regionkey = region.r_regionkey
ORDER BY
  region.r_name
