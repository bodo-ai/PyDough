WITH _s2 AS (
  SELECT
    part_size
  FROM GENERATE_SERIES(1, 59, 5) AS sizes(part_size)
), _s3 AS (
  SELECT
    _s0.part_size,
    COUNT(*) AS n_rows
  FROM _s2 AS _s0
  JOIN tpch.part AS part
    ON _s0.part_size <= part.p_size
    AND part.p_name LIKE '%almond%'
    AND part.p_size <= (
      _s0.part_size + 4
    )
  GROUP BY
    1
)
SELECT
  _s2.part_size,
  COALESCE(_s3.n_rows, 0) AS n_parts
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.part_size = _s3.part_size
