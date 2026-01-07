WITH _s1 AS (
  SELECT
    p_size,
    COUNT(*) AS n_rows
  FROM tpch.PART
  WHERE
    p_name LIKE '%turquoise%'
  GROUP BY
    1
)
SELECT
  sizes.part_size,
  COALESCE(_s1.n_rows, 0) AS n_parts
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
  ROW(56),
  ROW(61),
  ROW(66),
  ROW(71),
  ROW(76),
  ROW(81),
  ROW(86),
  ROW(91),
  ROW(96)) AS sizes(part_size)
LEFT JOIN _s1 AS _s1
  ON _s1.p_size = sizes.part_size
