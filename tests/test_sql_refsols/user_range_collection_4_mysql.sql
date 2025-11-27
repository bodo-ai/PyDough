WITH _q_0 AS (
  SELECT
    1 AS `1`
  UNION ALL
  SELECT
    2 AS `2`
  UNION ALL
  SELECT
    3 AS `3`
  UNION ALL
  SELECT
    4 AS `4`
  UNION ALL
  SELECT
    5 AS `5`
  UNION ALL
  SELECT
    6 AS `6`
  UNION ALL
  SELECT
    7 AS `7`
  UNION ALL
  SELECT
    8 AS `8`
  UNION ALL
  SELECT
    9 AS `9`
  UNION ALL
  SELECT
    10 AS `10`
), _t AS (
  SELECT
    PART.p_name,
    PART.p_retailprice,
    _q_0.`1` AS part_size,
    ROW_NUMBER() OVER (PARTITION BY _q_0.`1` ORDER BY CASE WHEN PART.p_retailprice IS NULL THEN 1 ELSE 0 END, PART.p_retailprice) AS _w
  FROM _q_0 AS _q_0
  JOIN tpch.PART AS PART
    ON PART.p_container LIKE '%SM DRUM%'
    AND PART.p_name LIKE '%azure%'
    AND PART.p_size = _q_0.`1`
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
