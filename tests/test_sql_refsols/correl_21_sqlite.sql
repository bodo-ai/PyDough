WITH _t2 AS (
  SELECT
    p_size
  FROM tpch.part
), _t1 AS (
  SELECT
    COUNT(*) AS n_parts
  FROM _t2
  GROUP BY
    p_size
), _s0 AS (
  SELECT
    AVG(n_parts) AS avg_n_parts
  FROM _t1
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM _t2
  GROUP BY
    p_size
)
SELECT
  COUNT(*) AS n_sizes
FROM _s0 AS _s0
JOIN _s1 AS _s1
  ON _s0.avg_n_parts < _s1.n_rows
