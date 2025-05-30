WITH _s0 AS (
  SELECT
    p_partkey AS key,
    CONCAT_WS('', 'old size: ', CAST(p_size AS TEXT)) AS message,
    CAST(CAST(p_size AS REAL) / 2.5 AS REAL) AS reduced_size,
    CAST(p_retailprice AS INTEGER) AS retail_price_int
  FROM tpch.part
  ORDER BY
    retail_price_int
  LIMIT 2
)
SELECT
  _s0.reduced_size,
  _s0.retail_price_int,
  _s0.message,
  lineitem.l_discount AS discount,
  STRFTIME('%d-%m-%Y', lineitem.l_receiptdate) AS date_dmy,
  STRFTIME('%m/%d', lineitem.l_receiptdate) AS date_md,
  STRFTIME('%H:%M%p', lineitem.l_receiptdate) AS am_pm
FROM _s0 AS _s0
JOIN tpch.lineitem AS lineitem
  ON _s0.key = lineitem.l_partkey
ORDER BY
  discount DESC
LIMIT 5
