WITH _q_0 AS (
  SELECT
    r_regionkey AS key,
    r_name AS name
  FROM tpch.region
), _s2 AS (
  SELECT
    _q_0.key,
    COUNT(*) AS n_rows
  FROM _q_0 AS _q_0
  CROSS JOIN LATERAL POSEXPLODE(SPLIT(_q_0.name, '\\QE\\E')) AS _s0(idx, val)
  GROUP BY
    1
), _s5 AS (
  SELECT
    _q_1.key,
    COUNT(*) AS n_rows
  FROM _q_0 AS _q_1
  CROSS JOIN LATERAL POSEXPLODE(SPLIT(_q_1.name, '\\QI\\E')) AS _s3(idx, val)
  GROUP BY
    1
), _s8 AS (
  SELECT
    _q_2.key,
    COUNT(*) AS n_rows
  FROM _q_0 AS _q_2
  CROSS JOIN LATERAL POSEXPLODE(SPLIT(_q_2.name, '\\Q \\E')) AS _s6(idx, val)
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  COALESCE(_s2.n_rows, 0) AS n_e_chunks,
  COALESCE(_s5.n_rows, 0) AS n_i_chunks,
  COALESCE(_s8.n_rows, 0) AS n_space_chunks
FROM tpch.region AS region
LEFT JOIN _s2 AS _s2
  ON _s2.key = region.r_regionkey
LEFT JOIN _s5 AS _s5
  ON _s5.key = region.r_regionkey
LEFT JOIN _s8 AS _s8
  ON _s8.key = region.r_regionkey
ORDER BY
  1
