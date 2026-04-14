WITH _t0 AS (
  SELECT
    part.p_name,
    part.p_retailprice,
    column1 AS part_size
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
    ON column1 = part.p_size
    AND part.p_container LIKE '%SM DRUM%'
    AND part.p_name LIKE '%azure%'
    AND part.p_type LIKE '%PLATED%'
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY column1 ORDER BY part.p_retailprice NULLS LAST) = 1
)
SELECT
  part_size,
  p_name AS name,
  p_retailprice AS retail_price
FROM _t0
ORDER BY
  1
