WITH _t1 AS (
  SELECT
    SUM(
      _s0.l_extendedprice * (
        1 - _s0.l_discount
      ) - _s13.ps_supplycost * _s0.l_quantity
    ) AS agg_0,
    _s5.n_name AS nation_name,
    EXTRACT(YEAR FROM _s10.o_orderdate) AS o_year
  FROM tpch.lineitem AS _s0
  JOIN tpch.part AS _s1
    ON _s0.l_partkey = _s1.p_partkey AND _s1.p_name LIKE '%green%'
  JOIN tpch.supplier AS _s4
    ON _s0.l_suppkey = _s4.s_suppkey
  JOIN tpch.nation AS _s5
    ON _s4.s_nationkey = _s5.n_nationkey
  JOIN tpch.orders AS _s10
    ON _s0.l_orderkey = _s10.o_orderkey
  JOIN tpch.partsupp AS _s13
    ON _s0.l_partkey = _s13.ps_partkey AND _s0.l_suppkey = _s13.ps_suppkey
  GROUP BY
    _s5.n_name,
    EXTRACT(YEAR FROM _s10.o_orderdate)
)
SELECT
  nation_name AS NATION,
  o_year AS O_YEAR,
  COALESCE(agg_0, 0) AS AMOUNT
FROM _t1
ORDER BY
  nation,
  o_year DESC
LIMIT 10
