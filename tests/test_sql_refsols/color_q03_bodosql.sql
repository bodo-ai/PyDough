WITH _s3 AS (
  SELECT
    clrs.colorname,
    shpmnts.comid,
    ANY_VALUE(clrs.chex) AS anything_chex,
    SUM(shpmnts.prc) AS sum_prc
  FROM clrs AS clrs
  JOIN shpmnts AS shpmnts
    ON clrs.identname = shpmnts.colid
  WHERE
    clrs.b >= (
      GREATEST(clrs.r, clrs.g) + 50
    )
  GROUP BY
    1,
    2
), _t2 AS (
  SELECT
    _s3.anything_chex,
    _s3.colorname,
    _s3.sum_prc,
    supls.supid
  FROM supls AS supls
  JOIN _s3 AS _s3
    ON _s3.comid = supls.supid
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY supls.supid ORDER BY COALESCE(_s3.sum_prc, 0) DESC, _s3.colorname) = 1
)
SELECT
  supls.supname AS company_name,
  _t2.colorname AS color_name,
  _t2.anything_chex AS color_hex,
  COALESCE(_t2.sum_prc, 0) AS total_profit
FROM supls AS supls
LEFT JOIN _t2 AS _t2
  ON _t2.supid = supls.supid
ORDER BY
  1 NULLS FIRST
