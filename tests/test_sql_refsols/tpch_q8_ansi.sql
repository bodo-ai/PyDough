WITH _t0 AS (
  SELECT
    SUM(
      CASE
        WHEN nation_2.n_name = 'BRAZIL'
        THEN lineitem.l_extendedprice * (
          1 - lineitem.l_discount
        )
        ELSE 0
      END
    ) AS sum_brazil_volume,
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS sum_volume,
    EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) AS o_year
  FROM tpch.lineitem AS lineitem
  JOIN tpch.part AS part
    ON lineitem.l_partkey = part.p_partkey AND part.p_type = 'ECONOMY ANODIZED STEEL'
  JOIN tpch.orders AS orders
    ON EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) IN (1995, 1996)
    AND lineitem.l_orderkey = orders.o_orderkey
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AMERICA'
  JOIN tpch.supplier AS supplier
    ON lineitem.l_suppkey = supplier.s_suppkey
  JOIN tpch.nation AS nation_2
    ON nation_2.n_nationkey = supplier.s_nationkey
  GROUP BY
    EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME))
)
SELECT
  o_year AS O_YEAR,
  COALESCE(sum_brazil_volume, 0) / COALESCE(sum_volume, 0) AS MKT_SHARE
FROM _t0
