WITH _s2 AS (
  SELECT
    sizes.part_size
  FROM (VALUES
    ROW(1),
    ROW(6),
    ROW(11),
    ROW(16),
    ROW(21),
    ROW(26),
    ROW(31),
    ROW(36),
    ROW(41),
    ROW(46),
    ROW(51),
    ROW(56)) AS sizes(part_size)
), _s3 AS (
  SELECT
    _s0.part_size,
    COUNT(*) AS n_rows
  FROM _s2 AS _s0
  JOIN tpch.PART AS PART
    ON PART.p_name LIKE '%almond%'
    AND PART.p_size <= (
      _s0.part_size + 4
    )
    AND PART.p_size >= _s0.part_size
  GROUP BY
    1
)
SELECT
  _s2.part_size,
  COALESCE(_s3.n_rows, 0) AS n_parts
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.part_size = _s3.part_size
