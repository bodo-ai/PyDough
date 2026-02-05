WITH _S1 AS (
  SELECT
    n_name AS N_NAME,
    n_regionkey AS N_REGIONKEY
  FROM TPCH.NATION
  WHERE
    n_nationkey = 4
)
SELECT
  REGION.r_name AS name,
  _S1.N_NAME AS nation_4_name
FROM TPCH.REGION REGION
LEFT JOIN _S1 _S1
  ON REGION.r_regionkey = _S1.N_REGIONKEY
