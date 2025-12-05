WITH _s0 AS (
  SELECT
    p_partkey,
    p_retailprice,
    p_size
  FROM tpch.part
  ORDER BY
    CAST(p_retailprice AS INTEGER)
  LIMIT 2
)
SELECT
  CAST(CAST(_s0.p_size AS REAL) / 2.5 AS REAL) AS reduced_size,
  CAST(_s0.p_retailprice AS INTEGER) AS retail_price_int,
  CONCAT_WS('', 'old size: ', CAST(_s0.p_size AS TEXT)) AS message,
  lineitem.l_discount AS discount,
  STRFTIME('%d-%m-%Y', lineitem.l_receiptdate) AS date_dmy,
  STRFTIME('%m/%d', lineitem.l_receiptdate) AS date_md,
  STRFTIME('%H:%M%p', lineitem.l_receiptdate) AS am_pm
FROM _s0 AS _s0
JOIN tpch.lineitem AS lineitem
  ON _s0.p_partkey = lineitem.l_partkey
ORDER BY
  4 DESC
LIMIT 5
