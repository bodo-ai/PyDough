WITH _s3 AS (
  SELECT
    sizes_2.part_size,
    COUNT(*) AS n_rows
  FROM (VALUES
    (1),
    (6),
    (11),
    (16),
    (21),
    (26),
    (31),
    (36),
    (41),
    (46),
    (51),
    (56)) AS sizes_2(part_size)
  JOIN tpch.part AS part
    ON part.p_name LIKE '%almond%'
    AND part.p_size <= (
      sizes_2.part_size + 4
    )
    AND part.p_size >= sizes_2.part_size
  GROUP BY
    1
)
SELECT
  sizes.part_size,
  COALESCE(_s3.n_rows, 0) AS n_parts
FROM (VALUES
  (1),
  (6),
  (11),
  (16),
  (21),
  (26),
  (31),
  (36),
  (41),
  (46),
  (51),
  (56)) AS sizes(part_size)
LEFT JOIN _s3 AS _s3
  ON _s3.part_size = sizes.part_size
