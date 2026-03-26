WITH "_t" AS (
  SELECT
    nation.n_name,
    nation.n_nationkey,
    regions_t25.rkey,
    regions_t25.rname,
    ROW_NUMBER() OVER (PARTITION BY regions_t25.rkey ORDER BY nation.n_nationkey NULLS FIRST) AS "_w"
  FROM regions_t25 regions_t25
  JOIN tpch.nation nation
    ON nation.n_regionkey = regions_t25.rkey
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
