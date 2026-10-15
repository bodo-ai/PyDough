WITH _s1 AS (
  SELECT
    r_regionkey AS key,
    r_name AS name
  FROM tpch.region
), _s3 AS (
  SELECT
    _s1.key,
    COUNT(*) AS n_rows
  FROM _s1 AS _s1
  CROSS JOIN LATERAL (
    SELECT
      UNNEST(STR_SPLIT(_s1.name, 'E')) AS _col_0,
      GENERATE_SUBSCRIPTS(STR_SPLIT(_s1.name, 'E'), 1) - 1 AS _col_1
  ) AS _s0(val, idx)
  GROUP BY
    1
), _s7 AS (
  SELECT
    _s5.key,
    COUNT(*) AS n_rows
  FROM _s1 AS _s5
  CROSS JOIN LATERAL (
    SELECT
      UNNEST(STR_SPLIT(_s5.name, 'I')) AS _col_0,
      GENERATE_SUBSCRIPTS(STR_SPLIT(_s5.name, 'I'), 1) - 1 AS _col_1
  ) AS _s4(val, idx)
  GROUP BY
    1
), _s11 AS (
  SELECT
    _s9.key,
    COUNT(*) AS n_rows
  FROM _s1 AS _s9
  CROSS JOIN LATERAL (
    SELECT
      UNNEST(STR_SPLIT(_s9.name, ' ')) AS _col_0,
      GENERATE_SUBSCRIPTS(STR_SPLIT(_s9.name, ' '), 1) - 1 AS _col_1
  ) AS _s8(val, idx)
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  COALESCE(_s3.n_rows, 0) AS n_e_chunks,
  COALESCE(_s7.n_rows, 0) AS n_i_chunks,
  COALESCE(_s11.n_rows, 0) AS n_space_chunks
FROM tpch.region AS region
LEFT JOIN _s3 AS _s3
  ON _s3.key = region.r_regionkey
LEFT JOIN _s7 AS _s7
  ON _s7.key = region.r_regionkey
LEFT JOIN _s11 AS _s11
  ON _s11.key = region.r_regionkey
ORDER BY
  1 NULLS FIRST
