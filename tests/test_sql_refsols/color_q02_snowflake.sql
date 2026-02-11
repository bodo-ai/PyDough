WITH _t1 AS (
  SELECT
    clrs."chex",
    clrs."colorname",
    shpmnts."comid",
    shpmnts."dos"
  FROM shpmnts AS shpmnts
  JOIN clrs AS clrs
    ON (
      LEAST(clrs."r", clrs."g", clrs."b") + 10
    ) >= GREATEST(clrs."r", clrs."g", clrs."b")
    AND GREATEST(clrs."r", clrs."g", clrs."b") <= 200
    AND LEAST(clrs."r", clrs."g", clrs."b") >= 100
    AND clrs."identname" = shpmnts."colid"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY shpmnts."comid" ORDER BY shpmnts."dos", shpmnts."sid") = 1
)
SELECT
  supls."supname" AS company_name,
  _t1."colorname" AS color_name,
  _t1."chex" AS color_hex,
  _t1."dos" AS ship_date
FROM supls AS supls
LEFT JOIN _t1 AS _t1
  ON _t1."comid" = supls."supid"
ORDER BY
  1 NULLS FIRST
