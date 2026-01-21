WITH _s0 AS (
  SELECT
    thresholds_collection.region_name,
    thresholds_collection.min_account_balance
  FROM (VALUES
    ROW('AFRICA', 5000.32),
    ROW('AMERICA', 8000.0),
    ROW('ASIA', 4600.32),
    ROW('EUROPE', 6400.5),
    ROW('MIDDLE EAST', 8999.99)) AS thresholds_collection(region_name, min_account_balance)
), _s1 AS (
  SELECT
    s_acctbal,
    s_nationkey
  FROM tpch.SUPPLIER
), _s2 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM tpch.NATION
), _s3 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.REGION
), _s5 AS (
  SELECT
    _s2.n_nationkey,
    _s3.r_name
  FROM _s2 AS _s2
  JOIN _s3 AS _s3
    ON _s2.n_regionkey = _s3.r_regionkey
), _s12 AS (
  SELECT DISTINCT
    _s5.r_name
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.min_account_balance < _s1.s_acctbal
  JOIN _s5 AS _s5
    ON _s0.region_name = _s5.r_name AND _s1.s_nationkey = _s5.n_nationkey
), _s11 AS (
  SELECT
    _s8.n_nationkey,
    _s9.r_name
  FROM _s2 AS _s8
  JOIN _s3 AS _s9
    ON _s8.n_regionkey = _s9.r_regionkey
), _s13 AS (
  SELECT
    _s11.r_name,
    COUNT(*) AS n_rows
  FROM _s0 AS _s6
  JOIN _s1 AS _s7
    ON _s6.min_account_balance < _s7.s_acctbal
  JOIN _s11 AS _s11
    ON _s11.n_nationkey = _s7.s_nationkey AND _s11.r_name = _s6.region_name
  GROUP BY
    1
)
SELECT
  _s12.r_name AS sup_region_name,
  _s13.n_rows AS n_suppliers
FROM _s12 AS _s12
JOIN _s13 AS _s13
  ON _s12.r_name = _s13.r_name
