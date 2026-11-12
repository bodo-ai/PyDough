WITH _t0 AS (
  SELECT
    part.p_name,
    part.p_retailprice,
    sizes.part_size
  FROM (VALUES
    (1),
    (2),
    (3),
    (4),
    (5),
    (6),
    (7),
    (8),
    (9),
    (10)) AS sizes(part_size)
  JOIN tpch.part AS part
    ON part.p_container LIKE '%SM DRUM%'
    AND part.p_name LIKE '%azure%'
    AND part.p_size = sizes.part_size
    AND part.p_type LIKE '%PLATED%'
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY sizes.part_size ORDER BY part.p_retailprice) = 1
)
SELECT
  part_size,
  p_name AS name,
  p_retailprice AS retail_price
FROM _t0
ORDER BY
  1 NULLS FIRST
