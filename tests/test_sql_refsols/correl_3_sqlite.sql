WITH _s4 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.region
), _t1 AS (
  SELECT
    MAX(_s0.r_regionkey) AS anything_r_regionkey
  FROM _s4 AS _s0
  JOIN tpch.nation AS nation
    ON _s0.r_regionkey = nation.n_regionkey
  JOIN tpch.customer AS customer
    ON LOWER(SUBSTRING(_s0.r_name, 1, 2)) = SUBSTRING(customer.c_comment, 1, 2)
    AND customer.c_nationkey = nation.n_nationkey
  GROUP BY
    nation.n_nationkey,
    _s0.r_regionkey
), _s5 AS (
  SELECT
    COUNT(*) AS n_rows,
    anything_r_regionkey
  FROM _t1
  GROUP BY
    anything_r_regionkey
)
SELECT
  _s4.r_name AS region_name,
  COALESCE(_s5.n_rows, 0) AS n_nations
FROM _s4 AS _s4
LEFT JOIN _s5 AS _s5
  ON _s4.r_regionkey = _s5.anything_r_regionkey
ORDER BY
  _s4.r_name
