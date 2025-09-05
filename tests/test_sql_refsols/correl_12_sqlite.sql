WITH _s0 AS (
  SELECT
    AVG(p_retailprice) AS avg_p_retailprice
  FROM tpch.part
), _s1 AS (
  SELECT
    AVG(p_retailprice) AS brand_avg_price,
    p_brand
  FROM tpch.part
  GROUP BY
    2
), _t0 AS (
  SELECT DISTINCT
    _s1.p_brand
  FROM _s0 AS _s0
  CROSS JOIN _s1 AS _s1
  JOIN tpch.part AS part
    ON _s0.avg_p_retailprice > part.p_retailprice
    AND _s1.brand_avg_price < part.p_retailprice
    AND _s1.p_brand = part.p_brand
    AND part.p_size < 3
)
SELECT
  p_brand AS brand
FROM _t0
ORDER BY
  1
