WITH _s3 AS (
  SELECT
    sizes_2.part_size,
    COUNT(*) AS n_rows
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
    ROW(56)) AS sizes_2(part_size)
  JOIN tpch.PART AS PART
    ON PART.p_name LIKE '%almond%'
    AND PART.p_size <= (
      sizes_2.part_size + 4
    )
    AND PART.p_size >= sizes_2.part_size
  GROUP BY
    1
)
SELECT
  sizes.part_size,
  COALESCE(_s3.n_rows, 0) AS n_parts
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
LEFT JOIN _s3 AS _s3
  ON _s3.part_size = sizes.part_size
