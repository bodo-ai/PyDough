WITH _s14 AS (
  SELECT
    SUM(
      IIF(
        nation.n_name = 'BRAZIL',
        lineitem.l_extendedprice * (
          1 - lineitem.l_discount
        ),
        0
      )
    ) AS agg_0,
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS agg_1,
    orders.o_custkey AS customer_key,
    CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) AS o_year
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
    AND orders.o_orderdate <= '1996-12-31'
    AND orders.o_orderdate >= '1995-01-01'
  GROUP BY
    orders.o_custkey,
    CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER)
), _t0 AS (
  SELECT
    SUM(_s14.agg_0) AS agg_0,
    SUM(_s14.agg_1) AS agg_1,
    _s14.o_year
  FROM _s14 AS _s14
  JOIN tpch.customer AS customer
    ON _s14.customer_key = customer.c_custkey
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AMERICA'
  GROUP BY
    _s14.o_year
)
SELECT
  o_year AS O_YEAR,
  CAST(COALESCE(agg_0, 0) AS REAL) / COALESCE(agg_1, 0) AS MKT_SHARE
FROM _t0
