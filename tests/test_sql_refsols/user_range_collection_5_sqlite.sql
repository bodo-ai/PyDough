WITH _s3 AS (
  SELECT
    sizes_2.column1 AS part_size,
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
    (56)) AS sizes_2
  JOIN tpch.part AS part
    ON part.p_name LIKE '%almond%'
    AND part.p_size <= (
      sizes_2.column1 + 4
    )
    AND part.p_size >= sizes_2.column1
  GROUP BY
    1
)
SELECT
  sizes.column1 AS part_size,
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
  (56)) AS sizes
LEFT JOIN _s3 AS _s3
  ON _s3.part_size = sizes.column1
