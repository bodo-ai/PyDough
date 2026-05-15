WITH "_S4" AS (
  SELECT
    r_name AS R_NAME,
    r_regionkey AS R_REGIONKEY
  FROM TPCH.REGION
), "_S5" AS (
  SELECT
    "_S0".R_REGIONKEY,
    COUNT(*) AS N_ROWS
  FROM "_S4" "_S0"
  JOIN "_S4" "_S1"
    ON "_S0".R_NAME <> "_S1".R_NAME
  JOIN TPCH.NATION NATION
    ON NATION.n_regionkey = "_S1".R_REGIONKEY
    AND SUBSTR(NATION.n_name, 1, 1) = SUBSTR("_S0".R_NAME, 1, 1)
  GROUP BY
    "_S0".R_REGIONKEY
)
SELECT
  "_S4".R_NAME AS region_name,
  COALESCE("_S5".N_ROWS, 0) AS n_other_nations
FROM "_S4" "_S4"
LEFT JOIN "_S5" "_S5"
  ON "_S4".R_REGIONKEY = "_S5".R_REGIONKEY
ORDER BY
  1 NULLS FIRST
