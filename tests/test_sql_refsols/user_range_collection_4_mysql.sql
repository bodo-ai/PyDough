WITH _t AS (
  SELECT
    PART.p_name,
    PART.p_retailprice,
    sizes.part_size,
    ROW_NUMBER() OVER (PARTITION BY sizes.part_size ORDER BY CASE WHEN PART.p_retailprice IS NULL THEN 1 ELSE 0 END, PART.p_retailprice) AS _w
  FROM (VALUES
    ROW(1),
    ROW(2),
    ROW(3),
    ROW(4),
    ROW(5),
    ROW(6),
    ROW(7),
    ROW(8),
    ROW(9),
    ROW(10)) AS sizes(part_size)
  JOIN tpch.PART AS PART
    ON PART.p_container LIKE '%SM DRUM%'
    AND PART.p_name LIKE '%azure%'
    AND PART.p_size = sizes.part_size
    AND PART.p_type LIKE '%PLATED%'
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
