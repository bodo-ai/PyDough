WITH _s3 AS (
  SELECT
    ps_suppkey,
    COUNT(*) AS n_rows
  FROM tpch.partsupp
  GROUP BY
    1
), _t AS (
  SELECT
    supplier.s_name,
    NTILE(1000) OVER (PARTITION BY nation.n_regionkey ORDER BY _s3.n_rows, supplier.s_name) AS _w
  FROM tpch.nation AS nation
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN _s3 AS _s3
    ON _s3.ps_suppkey = supplier.s_suppkey
)
SELECT
  s_name AS name
FROM _t
WHERE
  _w = 1000
