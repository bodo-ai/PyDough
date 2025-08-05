WITH _s0 AS (
  SELECT
    GROUP_CONCAT(CASE WHEN r_name <> 'EUROPE' THEN r_name ELSE NULL END, ', ') AS agg_1,
    GROUP_CONCAT(r_name) AS combine_strings_r_name
  FROM tpch.region
), _s1 AS (
  SELECT
    GROUP_CONCAT(SUBSTRING(n_name, 1, 1), '') AS agg_2
  FROM tpch.nation
), _t2 AS (
  SELECT DISTINCT
    o_orderpriority
  FROM tpch.orders
  WHERE
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1992
), _s3 AS (
  SELECT
    GROUP_CONCAT(SUBSTRING(o_orderpriority, 3), ' <=> ') AS agg_3
  FROM _t2
)
SELECT
  _s0.combine_strings_r_name AS s1,
  _s0.agg_1 AS s2,
  _s1.agg_2 AS s3,
  _s3.agg_3 AS s4
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
LEFT JOIN _s3 AS _s3
  ON TRUE
