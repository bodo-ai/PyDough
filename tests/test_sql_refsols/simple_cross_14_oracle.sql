WITH "_S1" AS (
  SELECT
    n_regionkey AS N_REGIONKEY,
    COUNT(*) AS N_ROWS
  FROM TPCH.NATION
  WHERE
    SUBSTR(n_name, 1, 1) IN ('A', 'B', 'C')
  GROUP BY
    n_regionkey
)
SELECT
  REGION.r_name AS region_name,
  'foo' AS x,
  COALESCE("_S1".N_ROWS, 0) AS n
FROM TPCH.REGION REGION
LEFT JOIN "_S1" "_S1"
  ON REGION.r_regionkey = "_S1".N_REGIONKEY
ORDER BY
  1 NULLS FIRST
