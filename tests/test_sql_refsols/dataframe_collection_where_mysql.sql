WITH _s5 AS (
  SELECT
    NATION.n_nationkey,
    REGION.r_name
  FROM tpch.NATION AS NATION
  JOIN tpch.REGION AS REGION
    ON NATION.n_regionkey = REGION.r_regionkey
)
SELECT
  _s5.r_name AS sup_region_name,
  COUNT(*) AS n_suppliers
FROM (VALUES
  ROW('AFRICA', 5000.32),
  ROW('AMERICA', 8000.0),
  ROW('ASIA', 4600.32),
  ROW('EUROPE', 6400.5),
  ROW('MIDDLE EAST', 8999.99)) AS thresholds(region_name, min_account_balance)
JOIN tpch.SUPPLIER AS SUPPLIER
  ON SUPPLIER.s_acctbal > thresholds.min_account_balance
JOIN _s5 AS _s5
  ON SUPPLIER.s_nationkey = _s5.n_nationkey AND _s5.r_name = thresholds.region_name
GROUP BY
  1
