WITH _t AS (
  SELECT
    part.p_name,
    part.p_retailprice,
    sizes.column1 AS part_size,
    ROW_NUMBER() OVER (PARTITION BY sizes.column1 ORDER BY part.p_retailprice) AS _w
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
    (10)) AS sizes
  JOIN tpch.part AS part
    ON part.p_container LIKE '%SM DRUM%'
    AND part.p_name LIKE '%azure%'
    AND part.p_size = sizes.column1
    AND part.p_type LIKE '%PLATED%'
)
SELECT
  part_size,
  p_name AS name,
  p_retailprice AS retail_price
FROM _t
WHERE
  _w = 1
ORDER BY
  1
