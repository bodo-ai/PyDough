WITH sizes AS (
  SELECT
    1 + SEQ4() * 5 AS part_size
  FROM TABLE(GENERATOR(ROWCOUNT => 20))
), _s1 AS (
  SELECT
    p_size,
    COUNT(*) AS n_rows
  FROM tpch.part
  WHERE
    CONTAINS(p_name, 'turquoise')
  GROUP BY
    1
)
SELECT
  sizes.part_size,
  COALESCE(_s1.n_rows, 0) AS n_parts
FROM sizes AS sizes
LEFT JOIN _s1 AS _s1
  ON _s1.p_size = sizes.part_size
