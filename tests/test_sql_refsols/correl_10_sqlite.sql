WITH _u_0 AS (
  SELECT
    SUBSTRING(r_name, 1, 1) AS _u_1,
    r_regionkey AS _u_2
  FROM tpch.region
  GROUP BY
    1,
    2
)
SELECT
  nation.n_name AS name,
  NULL AS rname
FROM tpch.nation AS nation
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = SUBSTRING(nation.n_name, 1, 1) AND _u_0._u_2 = nation.n_regionkey
WHERE
  _u_0._u_1 IS NULL
ORDER BY
  1
