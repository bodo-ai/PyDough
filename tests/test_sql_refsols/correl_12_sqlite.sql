WITH _s0 AS (
  SELECT
    AVG(p_retailprice) AS global_avg_price
  FROM tpch.part
), _s1 AS (
  SELECT
    AVG(p_retailprice) AS brand_avg_price,
    p_brand
  FROM tpch.part
  GROUP BY
    p_brand
)
SELECT
  _s1.p_brand AS brand
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
WHERE
  NOT _u_0._u_1 IS NULL
ORDER BY
  _s1.p_brand
