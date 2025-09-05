WITH _t2 AS (
  SELECT
    p_brand,
    p_retailprice
  FROM tpch.part
), _s0 AS (
  SELECT
    AVG(p_retailprice) AS avg_p_retailprice,
    p_brand
  FROM _t2
  GROUP BY
    2
), _t0 AS (
  SELECT DISTINCT
    _s0.p_brand
  FROM _s0 AS _s0
  JOIN _t2 AS _s1
    ON _s0.p_brand = _s1.p_brand
    AND _s1.p_retailprice > (
      1.4 * _s0.avg_p_retailprice
    )
)
SELECT
  p_brand AS brand
FROM _t0
ORDER BY
  1
