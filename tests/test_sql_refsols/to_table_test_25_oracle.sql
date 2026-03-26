WITH "_t" AS (
  SELECT
    NATION.n_name,
    NATION.n_nationkey,
    REGIONS_T25.rkey,
    REGIONS_T25.rname,
    ROW_NUMBER() OVER (PARTITION BY REGIONS_T25.rkey ORDER BY NATION.n_nationkey NULLS FIRST) AS "_w"
  FROM REGIONS_T25 REGIONS_T25
  JOIN TPCH.NATION NATION
    ON NATION.n_regionkey = REGIONS_T25.rkey
)
SELECT
  rkey,
  rname,
  n_nationkey AS nkey,
  n_name AS nname
FROM "_t"
WHERE
  "_w" = 1
ORDER BY
  1 NULLS FIRST
