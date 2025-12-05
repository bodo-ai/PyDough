WITH _s0 AS (
  SELECT
    p_partkey,
    p_retailprice,
    p_size
  FROM tpch.part
  ORDER BY
    CAST(p_retailprice AS BIGINT)
  LIMIT 2
)
SELECT
  CAST(_s0.p_size / 2.5 AS DOUBLE) AS reduced_size,
  CAST(_s0.p_retailprice AS BIGINT) AS retail_price_int,
  CONCAT_WS('', 'old size: ', CAST(_s0.p_size AS TEXT)) AS message,
  lineitem.l_discount AS discount,
  TIME_TO_STR(lineitem.l_receiptdate, '%d-%m-%Y') AS date_dmy,
  TIME_TO_STR(lineitem.l_receiptdate, '%m/%d') AS date_md,
  TIME_TO_STR(lineitem.l_receiptdate, '%H:%M%p') AS am_pm
FROM _s0 AS _s0
JOIN tpch.lineitem AS lineitem
  ON _s0.p_partkey = lineitem.l_partkey
ORDER BY
  4 DESC
LIMIT 5
