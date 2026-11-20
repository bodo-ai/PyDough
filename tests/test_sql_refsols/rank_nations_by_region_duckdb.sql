SELECT
  nation.n_name AS name,
  RANK() OVER (ORDER BY region.r_name) AS rank
FROM main.nation AS nation
JOIN main.region AS region
  ON nation.n_regionkey = region.r_regionkey
