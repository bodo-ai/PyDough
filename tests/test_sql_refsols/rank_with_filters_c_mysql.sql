WITH _s0 AS (
  SELECT DISTINCT
    p_size
  FROM tpch.PART
  ORDER BY
    1 DESC
  LIMIT 5
), _t AS (
  SELECT
    PART.p_size AS size_1,
    PART.p_name,
    ROW_NUMBER() OVER (PARTITION BY _s0.p_size ORDER BY CASE WHEN PART.p_retailprice IS NULL THEN 1 ELSE 0 END DESC, PART.p_retailprice DESC, CASE WHEN PART.p_partkey IS NULL THEN 1 ELSE 0 END, PART.p_partkey) AS _w
  FROM _s0 AS _s0
  JOIN tpch.PART AS PART
    ON PART.p_size = _s0.p_size
)
SELECT
  p_name AS pname,
  size_1 AS psize
FROM _t
WHERE
  _w = 1
