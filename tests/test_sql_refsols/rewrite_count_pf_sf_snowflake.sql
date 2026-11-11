WITH _u_0 AS (
  SELECT
    c_nationkey AS _u_1
  FROM snowflake_sample_data.tpch_sf1.customer
  WHERE
    c_mktsegment = 'BUILDING'
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM snowflake_sample_data.tpch_sf1.nation AS nation
JOIN snowflake_sample_data.tpch_sf1.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = nation.n_nationkey
WHERE
  NOT _u_0._u_1 IS NULL
