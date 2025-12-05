WITH _s1 AS (
  SELECT
    p_size,
    COUNT(*) AS n_rows
  FROM tpch.part
  WHERE
    p_name LIKE '%turquoise%'
  GROUP BY
    1
)
SELECT
  sizes.part_size,
  COALESCE(_s1.n_rows, 0) AS n_parts
FROM GENERATE_SERIES(1, 99, 5) AS sizes(part_size)
LEFT JOIN _s1 AS _s1
  ON _s1.p_size = sizes.part_size
