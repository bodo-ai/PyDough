WITH _s7 AS (
  SELECT
    region.r_regionkey AS key
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AMERICA'
), _s9 AS (
  SELECT
    _s7.key,
    orders.o_orderdate AS order_date
  FROM tpch.orders AS orders
  JOIN _s7 AS _s7
    ON _s7.key = orders.o_custkey
  WHERE
    EXTRACT(YEAR FROM orders.o_orderdate) IN (1995, 1996)
), _t0 AS (
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
    EXTRACT(YEAR FROM _s9.order_date) AS o_year
  FROM tpch.lineitem AS lineitem
  JOIN tpch.part AS part
    ON lineitem.l_partkey = part.p_partkey AND part.p_type = 'ECONOMY ANODIZED STEEL'
  JOIN _s9 AS _s9
    ON _s9.key = lineitem.l_orderkey
  JOIN tpch.supplier AS supplier
    ON lineitem.l_suppkey = supplier.s_suppkey
  JOIN tpch.nation AS nation
    ON nation.n_nationkey = supplier.s_nationkey
  GROUP BY
    EXTRACT(YEAR FROM _s9.order_date)
)
SELECT
  o_year AS O_YEAR,
  COALESCE(agg_0, 0) / COALESCE(agg_1, 0) AS MKT_SHARE
FROM _t0
