WITH _t0 AS (
  SELECT
    SUM(
      CASE
        WHEN _s17.n_name = 'BRAZIL'
        THEN _s0.l_extendedprice * (
          1 - _s0.l_discount
        )
        ELSE 0
      END
    ) AS agg_0,
    SUM(_s0.l_extendedprice * (
      1 - _s0.l_discount
    )) AS agg_1,
    EXTRACT(YEAR FROM _s4.o_orderdate) AS o_year
  FROM tpch.lineitem AS _s0
  JOIN tpch.part AS _s1
    ON _s0.l_partkey = _s1.p_partkey AND _s1.p_type = 'ECONOMY ANODIZED STEEL'
  JOIN tpch.orders AS _s4
    ON EXTRACT(YEAR FROM _s4.o_orderdate) IN (1995, 1996)
    AND _s0.l_orderkey = _s4.o_orderkey
  JOIN tpch.customer AS _s5
    ON _s4.o_custkey = _s5.c_custkey
  JOIN tpch.nation AS _s6
    ON _s5.c_nationkey = _s6.n_nationkey
  JOIN tpch.region AS _s9
    ON _s6.n_regionkey = _s9.r_regionkey AND _s9.r_name = 'AMERICA'
  JOIN tpch.supplier AS _s16
    ON _s0.l_suppkey = _s16.s_suppkey
  JOIN tpch.nation AS _s17
    ON _s16.s_nationkey = _s17.n_nationkey
  GROUP BY
    EXTRACT(YEAR FROM _s4.o_orderdate)
)
SELECT
  o_year AS O_YEAR,
  COALESCE(agg_0, 0) / COALESCE(agg_1, 0) AS MKT_SHARE
FROM _t0
