WITH _s0 AS (
  SELECT
    p_partkey AS key,
    CONCAT_WS('', 'old size: ', CAST(p_size AS TEXT)) AS message,
    CAST(p_size / 2.5 AS DOUBLE) AS reduced_size,
    CAST(p_retailprice AS BIGINT) AS retail_price_int
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
  TIME_TO_STR(lineitem.l_receiptdate, '%d-%m-%Y') AS date_dmy,
  TIME_TO_STR(lineitem.l_receiptdate, '%m/%d') AS date_md,
  TIME_TO_STR(lineitem.l_receiptdate, '%H:%M%p') AS am_pm
FROM _s0 AS _s0
JOIN tpch.lineitem AS lineitem
  ON _s0.key = lineitem.l_partkey
ORDER BY
  discount DESC
LIMIT 5
