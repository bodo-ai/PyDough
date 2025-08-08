WITH _t3 AS (
  SELECT DISTINCT
    p_container,
    p_size,
    p_type
  FROM tpch.part
), _t2 AS (
  SELECT
    COUNT(*) AS n_combos
  FROM _t3
  GROUP BY
    p_size
), _s0 AS (
  SELECT
    AVG(n_combos) AS avg_n_combo
  FROM _t2
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM _t3
  GROUP BY
    p_size
)
SELECT
  COUNT(*) AS n_sizes
FROM _s0 AS _s0
JOIN _s1 AS _s1
  ON _s0.avg_n_combo < _s1.n_rows
