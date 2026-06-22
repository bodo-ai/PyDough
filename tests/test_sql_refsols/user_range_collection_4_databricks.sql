WITH _t0 AS (
  SELECT
    part.p_name,
    part.p_retailprice,
    sizes.part_size
  FROM VALUES
    (1),
    (2),
    (3),
    (4),
    (5),
    (6),
    (7),
    (8),
    (9),
    (10) AS sizes(part_size)
  JOIN tpch.part AS part
    ON CONTAINS(part.p_container, 'SM DRUM')
    AND CONTAINS(part.p_name, 'azure')
    AND CONTAINS(part.p_type, 'PLATED')
    AND part.p_size = sizes.part_size
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY sizes.part_size ORDER BY part.p_retailprice NULLS LAST) = 1
)
SELECT
  part_size,
  p_name AS name,
  p_retailprice AS retail_price
FROM _t0
ORDER BY
  1
