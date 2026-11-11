SELECT
  COUNT(*) AS n
FROM main.supplier AS supplier
JOIN main.nation AS nation
  ON nation.n_nationkey = supplier.s_nationkey
JOIN main.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
