WITH _s3 AS (
  SELECT
    ps_suppkey,
    COUNT(*) AS n_rows
  FROM tpch.partsupp
  GROUP BY
    1
), _t0 AS (
  SELECT
    supplier.s_name
  FROM tpch.nation AS nation
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN _s3 AS _s3
    ON _s3.ps_suppkey = supplier.s_suppkey
  QUALIFY
    NTILE(1000) OVER (PARTITION BY n_regionkey ORDER BY _s3.n_rows NULLS LAST, supplier.s_name NULLS LAST) = 1000
)
SELECT
  s_name AS name
FROM _t0
