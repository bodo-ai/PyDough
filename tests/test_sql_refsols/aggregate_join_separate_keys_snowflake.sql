WITH _s0 AS (
  SELECT
    c_mktsegment,
    c_nationkey,
    COUNT(*) AS n_rows
  FROM snowflake_sample_data.tpch_sf1.customer
  GROUP BY
    1,
    2
)
SELECT
  _s0.c_mktsegment AS market_segment,
  nation.n_name AS nation_name,
  SUM(_s0.n_rows) AS n
FROM _s0 AS _s0
JOIN snowflake_sample_data.tpch_sf1.nation AS nation
  ON _s0.c_nationkey = nation.n_nationkey
GROUP BY
  1,
  2
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
