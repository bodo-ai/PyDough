WITH _u_0 AS (
  SELECT
    SUBSTRING(n_name, 1, 1) AS _u_1,
    n_regionkey AS _u_2
  FROM tpch.nation
  GROUP BY
    1,
    2
)
SELECT
  region.r_name AS name,
  0 AS n_prefix_nations
FROM tpch.region AS region
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = SUBSTRING(region.r_name, 1, 1) AND _u_0._u_2 = region.r_regionkey
WHERE
  _u_0._u_1 IS NULL
