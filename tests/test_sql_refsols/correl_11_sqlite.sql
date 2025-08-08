WITH _t3 AS (
  SELECT
    p_brand,
    p_retailprice
  FROM tpch.part
), _s0 AS (
  SELECT
    AVG(p_retailprice) AS avg_price,
    p_brand
  FROM _t3
  GROUP BY
    p_brand
)
SELECT
  MAX(_s0.p_brand) AS brand
FROM _s0 AS _s0
JOIN _t3 AS _s1
  ON _s0.p_brand = _s1.p_brand AND _s1.p_retailprice > (
    1.4 * _s0.avg_price
  )
GROUP BY
  _s0.p_brand
ORDER BY
  MAX(_s0.p_brand)
