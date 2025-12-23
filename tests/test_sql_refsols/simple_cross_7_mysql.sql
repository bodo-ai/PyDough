WITH _s3 AS (
  SELECT
    PART.p_partkey,
    COUNT(*) AS n_rows
  FROM tpch.PART AS PART
  JOIN tpch.PART AS PART_2
    ON ABS(PART_2.p_retailprice - PART.p_retailprice) < 5.0
    AND PART.p_brand = PART_2.p_brand
    AND PART.p_mfgr = PART_2.p_mfgr
    AND PART.p_partkey < PART_2.p_partkey
    AND PART_2.p_name LIKE '%tomato%'
  WHERE
    PART.p_brand = 'Brand#35'
    AND PART.p_mfgr = 'Manufacturer#3'
    AND PART.p_name LIKE '%tomato%'
  GROUP BY
    1
)
SELECT
  PART.p_partkey AS original_part_key,
  COALESCE(_s3.n_rows, 0) AS n_other_parts
FROM tpch.PART AS PART
LEFT JOIN _s3 AS _s3
  ON PART.p_partkey = _s3.p_partkey
WHERE
  PART.p_brand = 'Brand#35'
  AND PART.p_mfgr = 'Manufacturer#3'
  AND PART.p_name LIKE '%tomato%'
ORDER BY
  2 DESC,
  1
LIMIT 5
