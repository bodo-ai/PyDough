WITH _t1 AS (
  SELECT
    SUM(_s0.l_extendedprice * (
      1 - _s0.l_discount
    )) AS agg_0,
    _s11.n_name AS cust_nation,
    CAST(STRFTIME('%Y', _s0.l_shipdate) AS INTEGER) AS l_year,
    _s2.n_name AS supp_nation
  FROM tpch.lineitem AS _s0
  JOIN tpch.supplier AS _s1
    ON _s0.l_suppkey = _s1.s_suppkey
  JOIN tpch.nation AS _s2
    ON _s1.s_nationkey = _s2.n_nationkey
  JOIN tpch.orders AS _s7
    ON _s0.l_orderkey = _s7.o_orderkey
  JOIN tpch.customer AS _s8
    ON _s7.o_custkey = _s8.c_custkey
  JOIN tpch.nation AS _s11
    ON _s11.n_nationkey = _s8.c_nationkey
  WHERE
    CAST(STRFTIME('%Y', _s0.l_shipdate) AS INTEGER) IN (1995, 1996)
    AND (
      _s11.n_name = 'FRANCE' OR _s11.n_name = 'GERMANY'
    )
    AND (
      _s11.n_name = 'FRANCE' OR _s2.n_name = 'FRANCE'
    )
    AND (
      _s11.n_name = 'GERMANY' OR _s2.n_name = 'GERMANY'
    )
    AND (
      _s2.n_name = 'FRANCE' OR _s2.n_name = 'GERMANY'
    )
  GROUP BY
    _s11.n_name,
    CAST(STRFTIME('%Y', _s0.l_shipdate) AS INTEGER),
    _s2.n_name
)
SELECT
  supp_nation AS SUPP_NATION,
  cust_nation AS CUST_NATION,
  l_year AS L_YEAR,
  COALESCE(agg_0, 0) AS REVENUE
FROM _t1
ORDER BY
  supp_nation,
  cust_nation,
  l_year
