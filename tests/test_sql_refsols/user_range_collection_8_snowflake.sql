WITH one_row AS (
  SELECT
    SEQ4() AS x
  FROM TABLE(GENERATOR(ROWCOUNT => 1)) AS _0
), _t0 AS (
  SELECT DISTINCT
    region.r_regionkey
  FROM snowflake_sample_data.tpch_sf1.region AS region
  JOIN snowflake_sample_data.tpch_sf1.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN one_row AS one_row
    ON one_row.x <= region.r_regionkey
)
SELECT
  r_regionkey AS rk
FROM _t0
ORDER BY
  1 NULLS FIRST
