WITH _t0 AS (
  SELECT
    nation.n_name,
    nation.n_nationkey,
    regions_t25.rkey,
    regions_t25.rname
  FROM e2e_tests_db.public.regions_t25 AS regions_t25
  JOIN snowflake_sample_data.tpch_sf1.nation AS nation
    ON nation.n_regionkey = regions_t25.rkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY regions_t25.rkey, regions_t25.rname ORDER BY nation.n_nationkey) = 1
)
SELECT
  rkey,
  rname,
  n_nationkey AS nkey,
  n_name AS nname
FROM _t0
ORDER BY
  1 NULLS FIRST
