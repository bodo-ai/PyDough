WITH _t AS (
  SELECT
    NATION.n_name,
    NATION.n_nationkey,
    regions_t25.rkey,
    regions_t25.rname,
    ROW_NUMBER() OVER (PARTITION BY regions_t25.rkey, regions_t25.rname ORDER BY CASE WHEN NATION.n_nationkey IS NULL THEN 1 ELSE 0 END, NATION.n_nationkey) AS _w
  FROM regions_t25 AS regions_t25
  JOIN tpch.NATION AS NATION
    ON NATION.n_regionkey = regions_t25.rkey
)
SELECT
  rkey,
  rname,
  n_nationkey AS nkey,
  n_name AS nname
FROM _t
WHERE
  _w = 1
ORDER BY
  1
