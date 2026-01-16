WITH _s5 AS (
  SELECT
    nation.n_nationkey,
    region.r_name
  FROM tpch.nation AS nation
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey
)
SELECT
  _s5.r_name AS sup_region_name,
  COUNT(*) AS n_suppliers
FROM (VALUES
  ('AFRICA', 5000.32),
  ('AMERICA', 8000.0),
  ('ASIA', 4600.32),
  ('EUROPE', 6400.5),
  ('MIDDLE EAST', 8999.99)) AS thresholds(region_name, min_account_balance)
JOIN tpch.supplier AS supplier
  ON supplier.s_acctbal > thresholds.min_account_balance
JOIN _s5 AS _s5
  ON _s5.n_nationkey = supplier.s_nationkey AND _s5.r_name = thresholds.region_name
GROUP BY
  1
