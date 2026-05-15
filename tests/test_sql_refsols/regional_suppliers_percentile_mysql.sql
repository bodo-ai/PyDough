WITH _s3 AS (
  SELECT
    ps_suppkey,
    COUNT(*) AS n_rows
  FROM tpch.PARTSUPP
  GROUP BY
    1
), _t AS (
  SELECT
    SUPPLIER.s_name,
    NTILE(1000) OVER (PARTITION BY NATION.n_regionkey ORDER BY CASE WHEN _s3.n_rows IS NULL THEN 1 ELSE 0 END, _s3.n_rows, CASE WHEN SUPPLIER.s_name COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, SUPPLIER.s_name COLLATE utf8mb4_bin) AS _w
  FROM tpch.NATION AS NATION
  JOIN tpch.SUPPLIER AS SUPPLIER
    ON NATION.n_nationkey = SUPPLIER.s_nationkey
  JOIN _s3 AS _s3
    ON SUPPLIER.s_suppkey = _s3.ps_suppkey
)
SELECT
  s_name AS name
FROM _t
WHERE
  _w = 1000
