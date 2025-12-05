WITH _s1 AS (
  SELECT
    c_nationkey,
    COUNT(*) AS n_rows
  FROM tpch.CUSTOMER
  GROUP BY
    1
)
SELECT
  NATION.n_name AS name,
  ROW_NUMBER() OVER (PARTITION BY NATION.n_regionkey ORDER BY CASE WHEN _s1.n_rows IS NULL THEN 1 ELSE 0 END DESC, _s1.n_rows DESC) AS `rank`
FROM tpch.NATION AS NATION
JOIN _s1 AS _s1
  ON NATION.n_nationkey = _s1.c_nationkey
ORDER BY
  2
LIMIT 5
