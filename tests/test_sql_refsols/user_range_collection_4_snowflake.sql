WITH sizes AS (
  SELECT
    1 + SEQ4() * 1 AS part_size
  FROM TABLE(GENERATOR(ROWCOUNT => 10))
), _t0 AS (
  SELECT
    part.p_name,
    part.p_retailprice,
    sizes.part_size
  FROM sizes AS sizes
  JOIN tpch.part AS part
    ON CONTAINS(part.p_container, 'SM DRUM')
    AND CONTAINS(part.p_name, 'azure')
    AND CONTAINS(part.p_type, 'PLATED')
    AND part.p_size = sizes.part_size
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY part_size ORDER BY part.p_retailprice) = 1
)
SELECT
  part_size,
  p_name AS name,
  p_retailprice AS retail_price
FROM _t0
ORDER BY
  1 NULLS FIRST
