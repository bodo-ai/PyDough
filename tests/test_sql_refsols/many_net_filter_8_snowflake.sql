SELECT
  COUNT(*) AS n
FROM snowflake_sample_data.tpch_sf1.supplier AS supplier
JOIN snowflake_sample_data.tpch_sf1.nation AS nation
  ON nation.n_nationkey = supplier.s_nationkey AND nation.n_regionkey = 0
JOIN snowflake_sample_data.tpch_sf1.nation AS nation_2
  ON nation.n_regionkey = nation_2.n_regionkey
JOIN snowflake_sample_data.tpch_sf1.customer AS customer
  ON customer.c_custkey = supplier.s_suppkey
  AND customer.c_nationkey = nation_2.n_nationkey
