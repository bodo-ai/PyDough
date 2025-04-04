WITH _t5_2 AS (
  SELECT
    SUM(l_quantity) AS agg_0,
    l_partkey AS part_key
  FROM tpch.lineitem
  WHERE
    l_shipdate < '1995-01-01' AND l_shipdate >= '1994-01-01'
  GROUP BY
    l_partkey
), _t7_2 AS (
  SELECT
    COUNT() AS agg_0,
    partsupp.ps_suppkey AS supplier_key
  FROM tpch.partsupp AS partsupp
  JOIN tpch.part AS part
    ON part.p_name LIKE 'forest%' AND part.p_partkey = partsupp.ps_partkey
  LEFT JOIN _t5_2 AS _t5
    ON _t5.part_key = part.p_partkey
  WHERE
    partsupp.ps_availqty > (
      COALESCE(_t5.agg_0, 0) * 0.5
    )
  GROUP BY
    partsupp.ps_suppkey
), _t0_2 AS (
  SELECT
    supplier.s_address AS s_address,
    supplier.s_name AS s_name,
    supplier.s_name AS ordering_1
  FROM tpch.supplier AS supplier
  LEFT JOIN tpch.nation AS nation
    ON nation.n_nationkey = supplier.s_nationkey
  LEFT JOIN _t7_2 AS _t7
    ON _t7.supplier_key = supplier.s_suppkey
  WHERE
    (
      COALESCE(_t7.agg_0, 0) AND nation.n_name = 'CANADA'
    ) > 0
  ORDER BY
    ordering_1
  LIMIT 10
)
SELECT
  _t0.s_name AS S_NAME,
  _t0.s_address AS S_ADDRESS
FROM _t0_2 AS _t0
ORDER BY
  _t0.ordering_1
