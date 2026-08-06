WITH _q_0 AS (
  SELECT
    r_regionkey AS key,
    r_name AS name
  FROM tpch.region
), _s1 AS (
  SELECT
    _q_0.key,
    COUNT(*) AS n_rows
  FROM _q_0 AS _q_0
  CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(_q_0.name, 'E')) WITH ORDINALITY AS _l(val, idx)
  GROUP BY
    1
), _s3 AS (
  SELECT
    _q_1.key,
    COUNT(*) AS n_rows
  FROM _q_0 AS _q_1
  CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(_q_1.name, 'I')) WITH ORDINALITY AS _l(val, idx)
  GROUP BY
    1
), _s5 AS (
  SELECT
    _q_2.key,
    COUNT(*) AS n_rows
  FROM _q_0 AS _q_2
  CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(_q_2.name, ' ')) WITH ORDINALITY AS _l(val, idx)
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  COALESCE(_s1.n_rows, 0) AS n_e_chunks,
  COALESCE(_s3.n_rows, 0) AS n_i_chunks,
  COALESCE(_s5.n_rows, 0) AS n_space_chunks
FROM tpch.region AS region
LEFT JOIN _s1 AS _s1
  ON _s1.key = region.r_regionkey
LEFT JOIN _s3 AS _s3
  ON _s3.key = region.r_regionkey
LEFT JOIN _s5 AS _s5
  ON _s5.key = region.r_regionkey
ORDER BY
  1 NULLS FIRST
