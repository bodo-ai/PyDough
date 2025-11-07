WITH sizes AS (
  SELECT
    1 + SEQ4() * 5 AS part_size
  FROM TABLE(GENERATOR(ROWCOUNT => 12))
), sizes_2 AS (
  SELECT
    1 + SEQ4() * 5 AS part_size
  FROM TABLE(GENERATOR(ROWCOUNT => 12))
), _s3 AS (
  SELECT
    sizes.part_size,
    COUNT(*) AS n_rows
  FROM sizes_2 AS sizes
  JOIN tpch.part AS part
    ON CONTAINS(part.p_name, 'almond')
    AND part.p_size <= (
      sizes.part_size + 4
    )
    AND part.p_size >= sizes.part_size
  GROUP BY
    1
)
SELECT
  sizes.part_size,
  COALESCE(_s3.n_rows, 0) AS n_parts
FROM sizes AS sizes
LEFT JOIN _s3 AS _s3
  ON _s3.part_size = sizes.part_size
