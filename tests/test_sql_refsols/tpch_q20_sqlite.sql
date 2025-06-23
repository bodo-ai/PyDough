WITH _s3 AS (
  SELECT
    COALESCE(SUM(l_quantity), 0) AS agg_0,
    l_partkey
  FROM tpch.lineitem
  WHERE
    CAST(STRFTIME('%Y', l_shipdate) AS INTEGER) = 1994
  GROUP BY
    l_partkey
), _s5 AS (
  SELECT
    _s3.agg_0,
    part.p_partkey
  FROM tpch.part AS part
  JOIN _s3 AS _s3
    ON _s3.l_partkey = part.p_partkey
  WHERE
    part.p_name LIKE 'forest%'
), _t1 AS (
  SELECT
    COUNT(*) AS n_rows,
    partsupp.ps_suppkey
  FROM tpch.partsupp AS partsupp
  JOIN _s5 AS _s5
    ON _s5.p_partkey = partsupp.ps_partkey
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
  ON _t1.n_rows > 0 AND _t1.ps_suppkey = supplier.s_suppkey
ORDER BY
  s_name
LIMIT 10
