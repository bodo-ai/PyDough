WITH "_S3" AS (
  SELECT
    PART.p_partkey AS P_PARTKEY,
    COUNT(*) AS N_ROWS
  FROM TPCH.PART PART
  JOIN TPCH.PART PART_2
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
    PART.p_partkey
)
SELECT
  PART.p_partkey AS original_part_key,
  COALESCE("_S3".N_ROWS, 0) AS n_other_parts
FROM TPCH.PART PART
LEFT JOIN "_S3" "_S3"
  ON PART.p_partkey = "_S3".P_PARTKEY
WHERE
  PART.p_brand = 'Brand#35'
  AND PART.p_mfgr = 'Manufacturer#3'
  AND PART.p_name LIKE '%tomato%'
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
