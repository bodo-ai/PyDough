WITH _t2 AS (
  SELECT
    colid,
    comid,
    cusid,
    dos
  FROM shpmnts
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY colid ORDER BY dos, sid) = 1
)
SELECT
  clrs.identname AS key,
  clrs.chex AS hex_code,
  CONCAT_WS(' ', cust.cstfname, cust.cstlname) AS first_customer,
  _t2.dos AS first_order_date,
  supls.supname AS first_order_company
FROM clrs AS clrs
JOIN _t2 AS _t2
  ON _t2.colid = clrs.identname
JOIN cust AS cust
  ON _t2.cusid = cust.cstid
JOIN supls AS supls
  ON _t2.comid = supls.supid
WHERE
  (
    clrs.r + clrs.g + clrs.b
  ) = 255
  AND GREATEST(clrs.r, clrs.g, clrs.b) = 255
  AND LEAST(clrs.r, clrs.g, clrs.b) = 0
ORDER BY
  1 NULLS FIRST
