WITH _s0 AS (
  SELECT
    p_partkey,
    p_retailprice,
    p_size
  FROM tpch.PART
  ORDER BY
    TRUNCATE(CAST(p_retailprice AS FLOAT), 0)
  LIMIT 2
)
SELECT
  CAST(_s0.p_size / 2.5 AS DOUBLE) AS reduced_size,
  TRUNCATE(CAST(_s0.p_retailprice AS FLOAT), 0) AS retail_price_int,
  CONCAT_WS('', 'old size: ', CAST(_s0.p_size AS CHAR)) AS message,
  LINEITEM.l_discount AS discount,
  DATE_FORMAT(LINEITEM.l_receiptdate, '%d-%m-%Y') COLLATE utf8mb4_bin AS date_dmy,
  DATE_FORMAT(LINEITEM.l_receiptdate, '%m/%d') AS date_md,
  DATE_FORMAT(LINEITEM.l_receiptdate, '%H:%i%p') AS am_pm
FROM _s0 AS _s0
JOIN tpch.LINEITEM AS LINEITEM
  ON LINEITEM.l_partkey = _s0.p_partkey
ORDER BY
  4 DESC,
  5
LIMIT 5
