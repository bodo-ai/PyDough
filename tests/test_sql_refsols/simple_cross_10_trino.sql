WITH _s4 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.region
), _s5 AS (
  SELECT
    _s0.r_regionkey,
    COUNT(*) AS n_rows
  FROM _s4 AS _s0
  JOIN _s4 AS _s1
    ON _s0.r_name <> _s1.r_name
  JOIN tpch.nation AS nation
    ON SUBSTRING(_s0.r_name, 1, 1) = SUBSTRING(nation.n_name, 1, 1)
    AND _s1.r_regionkey = nation.n_regionkey
  GROUP BY
    1
)
SELECT
  _s4.r_name AS region_name,
  COALESCE(_s5.n_rows, 0) AS n_other_nations
FROM _s4 AS _s4
LEFT JOIN _s5 AS _s5
  ON _s4.r_regionkey = _s5.r_regionkey
ORDER BY
  1 NULLS FIRST
