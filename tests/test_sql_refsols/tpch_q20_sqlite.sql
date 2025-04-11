WITH _s5 AS (
  SELECT
    SUM(l_quantity) AS agg_0,
    l_partkey AS part_key
  FROM tpch.lineitem
  WHERE
    l_shipdate < '1995-01-01' AND l_shipdate >= '1994-01-01'
  GROUP BY
    l_partkey
), _s7 AS (
  SELECT
    COUNT() AS agg_0,
    partsupp.ps_suppkey AS supplier_key
  FROM tpch.partsupp AS partsupp
  JOIN tpch.part AS part
    ON part.p_name LIKE 'forest%' AND part.p_partkey = partsupp.ps_partkey
  LEFT JOIN _s5 AS _s5
    ON _s5.part_key = part.p_partkey
  WHERE
    partsupp.ps_availqty > (
      COALESCE(_s5.agg_0, 0) * 0.5
    )
  GROUP BY
    partsupp.ps_suppkey
)
SELECT
  supplier.s_name AS S_NAME,
  supplier.s_address AS S_ADDRESS
FROM tpch.supplier AS supplier
LEFT JOIN tpch.nation AS nation
  ON nation.n_nationkey = supplier.s_nationkey
LEFT JOIN _s7 AS _s7
  ON _s7.supplier_key = supplier.s_suppkey
WHERE
  (
    COALESCE(_s7.agg_0, 0) AND nation.n_name = 'CANADA'
  ) > 0
ORDER BY
  s_name
LIMIT 10
