SELECT
  COUNT(*) AS n
FROM main.customer AS customer
JOIN main.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
JOIN main.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
