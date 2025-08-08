WITH _s1 AS (
  SELECT
    SUBSTRING(r_name, 1, 1) AS expr_0,
    r_name,
    r_regionkey
  FROM tpch.region
)
SELECT
  nation.n_name AS name,
  _s1.r_name AS rname
FROM tpch.nation AS nation
LEFT JOIN _s1 AS _s1
  ON _s1.expr_0 = SUBSTRING(nation.n_name, 1, 1)
  AND _s1.r_regionkey = nation.n_regionkey
ORDER BY
  nation.n_name
