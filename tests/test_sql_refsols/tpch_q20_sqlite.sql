WITH _s3 AS (
  SELECT
    SUM(l_quantity) AS sum_l_quantity,
    l_partkey
  FROM tpch.lineitem
  WHERE
    CAST(STRFTIME('%Y', l_shipdate) AS INTEGER) = 1994
  GROUP BY
    2
), _s5 AS (
  SELECT
    part.p_partkey,
    _s3.sum_l_quantity
  FROM tpch.part AS part
  JOIN _s3 AS _s3
    ON _s3.l_partkey = part.p_partkey
  WHERE
    part.p_name LIKE 'forest%'
), _s7 AS (
  SELECT DISTINCT
    partsupp.ps_suppkey
  FROM tpch.partsupp AS partsupp
  JOIN _s5 AS _s5
    ON _s5.p_partkey = partsupp.ps_partkey
    AND partsupp.ps_availqty > (
      0.5 * COALESCE(_s5.sum_l_quantity, 0)
    )
)
SELECT
  supplier.s_name AS S_NAME,
  supplier.s_address AS S_ADDRESS
FROM tpch.supplier AS supplier
JOIN tpch.nation AS nation
  ON nation.n_name = 'CANADA' AND nation.n_nationkey = supplier.s_nationkey
JOIN _s7 AS _s7
  ON _s7.ps_suppkey = supplier.s_suppkey
ORDER BY
  1
LIMIT 10
