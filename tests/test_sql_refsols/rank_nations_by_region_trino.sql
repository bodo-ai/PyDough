SELECT
  nation.n_name AS name,
  RANK() OVER (ORDER BY region.r_name) AS rank
FROM cassandra.tpch.nation AS nation
JOIN mongo.tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey
