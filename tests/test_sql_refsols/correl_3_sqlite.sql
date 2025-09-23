WITH _s4 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.region
), _t2 AS (
  SELECT
    _s0.r_regionkey,
    MAX(nation.n_regionkey) AS anything_n_regionkey
  FROM _s4 AS _s0
  JOIN tpch.nation AS nation
    ON _s0.r_regionkey = nation.n_regionkey
  JOIN tpch.customer AS customer
    ON LOWER(SUBSTRING(_s0.r_name, 1, 2)) = SUBSTRING(customer.c_comment, 1, 2)
    AND customer.c_nationkey = nation.n_nationkey
  GROUP BY
    nation.n_nationkey,
    1
), _s5 AS (
  SELECT
    r_regionkey,
    COUNT(*) AS n_rows
  FROM _t2
  WHERE
    anything_n_regionkey = r_regionkey
  GROUP BY
    1
)
SELECT
  _s4.r_name AS region_name,
  _s5.n_rows AS n_nations
FROM _s4 AS _s4
JOIN _s5 AS _s5
  ON _s4.r_regionkey = _s5.r_regionkey
ORDER BY
  1
