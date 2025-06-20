WITH _s2 AS (
  SELECT
    COUNT(*) AS agg_1,
    de_product_id AS product_id
  FROM main.devices
  GROUP BY
    de_product_id
), _s11 AS (
  SELECT
    SUM(_s2.agg_1) AS agg_1,
    EXTRACT(YEAR FROM _s1.pr_release) AS release_year
  FROM _s2 AS _s2
  JOIN main.products AS _s1
    ON _s1.pr_id = _s2.product_id
  GROUP BY
    EXTRACT(YEAR FROM _s1.pr_release)
), _s12 AS (
  SELECT
    COUNT(*) AS agg_0,
    EXTRACT(YEAR FROM _s5.pr_release) AS release_year
  FROM main.devices AS _s4
  JOIN main.products AS _s5
    ON _s4.de_product_id = _s5.pr_id
  JOIN main.incidents AS _s8
    ON _s4.de_id = _s8.in_device_id
  GROUP BY
    EXTRACT(YEAR FROM _s5.pr_release)
)
SELECT
  _s11.release_year AS year,
  ROUND(COALESCE(_s12.agg_0, 0) / _s11.agg_1, 2) AS ir
FROM _s11 AS _s11
LEFT JOIN _s12 AS _s12
  ON _s11.release_year = _s12.release_year
ORDER BY
  year
