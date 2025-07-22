WITH _t AS (
  SELECT
    part.p_name,
    part.p_retailprice,
    column1 AS part_size,
    ROW_NUMBER() OVER (PARTITION BY column1 ORDER BY part.p_retailprice) AS _w
  FROM (VALUES
    (0),
    (1),
    (2),
    (3),
    (4),
    (5),
    (6),
    (7),
    (8),
    (9)) AS sizes
  JOIN tpch.part AS part
    ON column1 = part.p_size AND part.p_name LIKE '%turquoise%'
)
SELECT
  part_size,
  p_name AS name,
  p_retailprice AS retail_price
FROM _t
WHERE
  _w = 1
ORDER BY
  part_size
