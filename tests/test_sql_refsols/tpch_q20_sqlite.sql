WITH _t5 AS (
  SELECT
    SUM(l_quantity) AS agg_0,
    l_partkey AS part_key
  FROM tpch.lineitem
  WHERE
    CAST(STRFTIME('%Y', l_shipdate) AS INTEGER) = 1994
  GROUP BY
    l_partkey
), _s5 AS (
  SELECT
    COALESCE(_t5.agg_0, 0) AS agg_0,
    part.p_partkey AS key
  FROM tpch.part AS part
  JOIN _t5 AS _t5
    ON _t5.part_key = part.p_partkey
  WHERE
    part.p_name LIKE 'forest%'
), _t1 AS (
  SELECT
    COUNT() AS agg_0,
    partsupp.ps_suppkey AS supplier_key
  FROM tpch.partsupp AS partsupp
  JOIN _s5 AS _s5
    ON _s5.key = partsupp.ps_partkey
    AND partsupp.ps_availqty > (
      0.5 * COALESCE(_s5.agg_0, 0)
    )
  GROUP BY
    partsupp.ps_suppkey
)
SELECT
  supplier.s_name AS S_NAME,
  supplier.s_address AS S_ADDRESS
FROM tpch.supplier AS supplier
JOIN tpch.nation AS nation
  ON nation.n_name = 'CANADA' AND nation.n_nationkey = supplier.s_nationkey
JOIN _t1 AS _t1
  ON _t1.agg_0 > 0 AND _t1.supplier_key = supplier.s_suppkey
ORDER BY
  s_name
LIMIT 10
