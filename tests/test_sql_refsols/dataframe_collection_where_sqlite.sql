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
  ('MIDDLE EAST', 8999.99)) AS thresholds
JOIN tpch.supplier AS supplier
  ON column2 < supplier.s_acctbal
JOIN _s5 AS _s5
  ON _s5.n_nationkey = supplier.s_nationkey AND _s5.r_name = column1
GROUP BY
  1
