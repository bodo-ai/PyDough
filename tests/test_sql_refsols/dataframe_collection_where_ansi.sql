WITH _s1 AS (
  SELECT
    s_acctbal,
    s_nationkey
  FROM tpch.supplier
), _s2 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM tpch.nation
), _s3 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.region
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
  FROM (VALUES
    ('AFRICA', 5000.32),
    ('AMERICA', 8000.0),
    ('ASIA', 4600.32),
    ('EUROPE', 6400.5),
    ('MIDDLE EAST', 8999.99)) AS thresholds_collection(region_name, min_account_balance)
  JOIN _s1 AS _s1
    ON _s1.s_acctbal > column2
  JOIN _s5 AS _s5
    ON _s1.s_nationkey = _s5.n_nationkey AND _s5.r_name = column1
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
  FROM (VALUES
    ('AFRICA', 5000.32),
    ('AMERICA', 8000.0),
    ('ASIA', 4600.32),
    ('EUROPE', 6400.5),
    ('MIDDLE EAST', 8999.99)) AS thresholds_collection_2(region_name, min_account_balance)
  JOIN _s1 AS _s7
    ON _s7.s_acctbal > column2
  JOIN _s11 AS _s11
    ON _s11.n_nationkey = _s7.s_nationkey AND _s11.r_name = column1
  GROUP BY
    1
)
SELECT
  _s12.r_name AS sup_region_name,
  _s13.n_rows AS n_suppliers
FROM _s12 AS _s12
JOIN _s13 AS _s13
  ON _s12.r_name = _s13.r_name
