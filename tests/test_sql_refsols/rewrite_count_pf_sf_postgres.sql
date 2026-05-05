WITH _u_0 AS (
  SELECT
    c_nationkey AS _u_1
  FROM tpch.customer
  WHERE
    c_mktsegment = 'BUILDING'
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = nation.n_nationkey
WHERE
  NOT _u_0._u_1 IS NULL
