WITH _S0 AS (
  SELECT
    n_name AS N_NAME,
    n_regionkey AS N_REGIONKEY
  FROM TPCH.NATION
  ORDER BY
    1 NULLS FIRST
  FETCH FIRST 2 ROWS ONLY
), _S1 AS (
  SELECT
    r_name AS R_NAME,
    r_regionkey AS R_REGIONKEY,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY r_regionkey ORDER BY r_regionkey DESC NULLS LAST) - 1.0
        ) - (
          (
            COUNT(r_regionkey) OVER (PARTITION BY r_regionkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN r_regionkey
      ELSE NULL
    END AS AVG_EXPR
  FROM TPCH.REGION
)
SELECT
  _S0.N_NAME AS nation_name,
  _S1.R_NAME AS a1,
  _S1.R_NAME AS a2,
  _S1.R_REGIONKEY AS a3,
  CASE
    WHEN NOT CASE WHEN _S1.R_NAME <> 'AMERICA' THEN _S1.R_REGIONKEY ELSE NULL END IS NULL
    THEN 1
    ELSE 0
  END AS a4,
  1 AS a5,
  _S1.R_REGIONKEY AS a6,
  _S1.R_NAME AS a7,
  _S1.AVG_EXPR AS a8
FROM _S0 _S0
JOIN _S1 _S1
  ON _S0.N_REGIONKEY = _S1.R_REGIONKEY
ORDER BY
  1 NULLS FIRST
