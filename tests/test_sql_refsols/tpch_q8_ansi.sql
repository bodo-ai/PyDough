WITH _t0_2 AS (
  SELECT
    SUM(
      CASE
        WHEN nation.n_name = 'BRAZIL'
        THEN lineitem.l_extendedprice * (
          1 - lineitem.l_discount
        )
        ELSE 0
      END
    ) AS agg_0,
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS agg_1,
    EXTRACT(YEAR FROM orders.o_orderdate) AS o_year
  FROM tpch.nation AS nation
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN tpch.partsupp AS partsupp
    ON partsupp.ps_suppkey = supplier.s_suppkey
  JOIN tpch.part AS part
    ON part.p_partkey = partsupp.ps_partkey AND part.p_type = 'ECONOMY ANODIZED STEEL'
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = partsupp.ps_partkey
    AND lineitem.l_suppkey = partsupp.ps_suppkey
  JOIN tpch.orders AS orders
    ON lineitem.l_orderkey = orders.o_orderkey
    AND orders.o_orderdate <= CAST('1996-12-31' AS DATE)
    AND orders.o_orderdate >= CAST('1995-01-01' AS DATE)
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
  JOIN tpch.nation AS nation_2
    ON customer.c_nationkey = nation_2.n_nationkey
  JOIN tpch.region AS region
    ON nation_2.n_regionkey = region.r_regionkey AND region.r_name = 'AMERICA'
  GROUP BY
    EXTRACT(YEAR FROM orders.o_orderdate)
)
SELECT
  _t0.o_year AS O_YEAR,
  COALESCE(_t0.agg_0, 0) / COALESCE(_t0.agg_1, 0) AS MKT_SHARE
FROM _t0_2 AS _t0
