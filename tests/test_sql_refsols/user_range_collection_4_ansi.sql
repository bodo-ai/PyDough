WITH _t0 AS (
  SELECT
    part.p_name,
    part.p_retailprice,
    column1 AS part_size
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
    (9)) AS sizes(_col_0)
  JOIN tpch.part AS part
    ON column1 = part.p_size AND part.p_name LIKE '%turquoise%'
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY column1 ORDER BY part.p_retailprice NULLS LAST) = 1
)
SELECT
  part_size,
  p_name AS name,
  p_retailprice AS retail_price
FROM _t0
ORDER BY
  part_size
