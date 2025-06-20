WITH _t5 AS (
  SELECT
    SUM(l_quantity) AS agg_0,
    l_partkey AS part_key
  FROM tpch.lineitem
  WHERE
    CAST(STRFTIME('%Y', l_shipdate) AS INTEGER) = 1994
  GROUP BY
    l_partkey
), _t1 AS (
  SELECT
    COUNT(*) AS agg_0,
    _s4.ps_suppkey AS supplier_key
  FROM tpch.partsupp AS _s4
  JOIN tpch.part AS _s5
    ON _s4.ps_partkey = _s5.p_partkey AND _s5.p_name LIKE 'forest%'
  JOIN _t5 AS _t5
    ON _s5.p_partkey = _t5.part_key
  WHERE
    _s4.ps_availqty > (
      0.5 * COALESCE(COALESCE(_t5.agg_0, 0), 0)
    )
  GROUP BY
    _s4.ps_suppkey
)
SELECT
  _s0.s_name AS S_NAME,
  _s0.s_address AS S_ADDRESS
FROM tpch.supplier AS _s0
JOIN tpch.nation AS _s1
  ON _s0.s_nationkey = _s1.n_nationkey AND _s1.n_name = 'CANADA'
JOIN _t1 AS _t1
  ON _s0.s_suppkey = _t1.supplier_key AND _t1.agg_0 > 0
ORDER BY
  s_name
LIMIT 10
