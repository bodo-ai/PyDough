WITH _s0 AS (
  SELECT
    AVG(p_retailprice) AS global_avg_price
  FROM tpch.part
), _s1 AS (
  SELECT
    AVG(p_retailprice) AS avg_p_retailprice,
    p_container
  FROM tpch.part
  GROUP BY
    p_type,
    2
)
SELECT
  _s1.p_container AS container,
  COUNT(*) AS n_types
FROM _s0 AS _s0
JOIN _s1 AS _s1
  ON _s0.global_avg_price < _s1.avg_p_retailprice
GROUP BY
  1
ORDER BY
  2 DESC,
  1
LIMIT 5
