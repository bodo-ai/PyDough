WITH _s5 AS (
  SELECT
    MAX(part.p_partkey) AS anything_p_partkey,
    SUM(lineitem.l_quantity) AS sum_l_quantity
  FROM tpch.part AS part
  JOIN tpch.lineitem AS lineitem
    ON EXTRACT(YEAR FROM CAST(lineitem.l_shipdate AS TIMESTAMP)) = 1994
    AND lineitem.l_partkey = part.p_partkey
  WHERE
    part.p_name LIKE 'forest%'
  GROUP BY
    lineitem.l_partkey
)
SELECT
  MAX(supplier.s_name) AS S_NAME,
  MAX(supplier.s_address) AS S_ADDRESS
FROM tpch.supplier AS supplier
JOIN tpch.nation AS nation
  ON nation.n_name = 'CANADA' AND nation.n_nationkey = supplier.s_nationkey
JOIN tpch.partsupp AS partsupp
  ON partsupp.ps_suppkey = supplier.s_suppkey
JOIN _s5 AS _s5
  ON _s5.anything_p_partkey = partsupp.ps_partkey
  AND partsupp.ps_availqty > (
    0.5 * COALESCE(_s5.sum_l_quantity, 0)
  )
GROUP BY
  partsupp.ps_suppkey
ORDER BY
  1 NULLS FIRST
LIMIT 10
