WITH _s1 AS (
  SELECT
    supid,
    supname
  FROM supls
), _s2 AS (
  SELECT
    shpmnts.comid,
    shpmnts.prc,
    shpmnts.vol
  FROM shpmnts AS shpmnts
  JOIN _s1 AS _s1
    ON _s1.supid = shpmnts.comid
  WHERE
    shpmnts.colid = 'brandeis_blue'
  ORDER BY
    ROUND(shpmnts.prc / shpmnts.vol, 2) NULLS FIRST,
    _s1.supname NULLS FIRST
  LIMIT 1
), _s6 AS (
  SELECT
    shpmnts.comid,
    shpmnts.prc,
    shpmnts.vol
  FROM shpmnts AS shpmnts
  JOIN _s1 AS _s5
    ON _s5.supid = shpmnts.comid
  WHERE
    shpmnts.colid = 'china_pink'
  ORDER BY
    ROUND(shpmnts.prc / shpmnts.vol, 2) NULLS FIRST,
    _s5.supname NULLS FIRST
  LIMIT 1
), _s12 AS (
  SELECT
    shpmnts.comid,
    shpmnts.prc,
    shpmnts.vol
  FROM shpmnts AS shpmnts
  JOIN _s1 AS _s11
    ON _s11.supid = shpmnts.comid
  WHERE
    shpmnts.colid = 'french_raspberry'
  ORDER BY
    ROUND(shpmnts.prc / shpmnts.vol, 2) NULLS FIRST,
    _s11.supname NULLS FIRST
  LIMIT 1
), _s18 AS (
  SELECT
    shpmnts.comid,
    shpmnts.prc,
    shpmnts.vol
  FROM shpmnts AS shpmnts
  JOIN _s1 AS _s17
    ON _s17.supid = shpmnts.comid
  WHERE
    shpmnts.colid = 'puce'
  ORDER BY
    ROUND(shpmnts.prc / shpmnts.vol, 2) NULLS FIRST,
    _s17.supname NULLS FIRST
  LIMIT 1
)
SELECT
  ROUND(_s2.prc / _s2.vol, 2) AS cheapest_brandeis_blue_price,
  _s3.supname AS cheapest_brandeis_blue_company,
  ROUND(_s6.prc / _s6.vol, 2) AS cheapest_china_pink_price,
  _s7.supname AS cheapest_china_pink_company,
  ROUND(_s12.prc / _s12.vol, 2) AS cheapest_french_raspberry_price,
  _s13.supname AS cheapest_french_raspberry_company,
  ROUND(_s18.prc / _s18.vol, 2) AS cheapest_puce_price,
  _s19.supname AS cheapest_puce_company
FROM _s2 AS _s2
JOIN _s1 AS _s3
  ON _s2.comid = _s3.supid
CROSS JOIN _s6 AS _s6
JOIN _s1 AS _s7
  ON _s6.comid = _s7.supid
CROSS JOIN _s12 AS _s12
JOIN _s1 AS _s13
  ON _s12.comid = _s13.supid
CROSS JOIN _s18 AS _s18
JOIN _s1 AS _s19
  ON _s18.comid = _s19.supid
