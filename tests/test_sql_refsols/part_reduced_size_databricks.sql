WITH _s0 AS (
  SELECT
    p_partkey,
    p_retailprice,
    p_size
  FROM tpch.part
  ORDER BY
    CAST(CAST(p_retailprice AS DOUBLE) AS BIGINT)
  LIMIT 2
)
SELECT
  CAST(_s0.p_size / 2.5 AS DOUBLE) AS reduced_size,
  CAST(CAST(_s0.p_retailprice AS DOUBLE) AS BIGINT) AS retail_price_int,
  CONCAT_WS('', 'old size: ', CAST(_s0.p_size AS STRING)) AS message,
  lineitem.l_discount AS discount,
  DATE_FORMAT(lineitem.l_receiptdate, 'dd-MM-yyyy') AS date_dmy,
  DATE_FORMAT(lineitem.l_receiptdate, 'MM/dd') AS date_md,
  DATE_FORMAT(lineitem.l_receiptdate, 'HH:mma') AS am_pm
FROM _s0 AS _s0
JOIN tpch.lineitem AS lineitem
  ON _s0.p_partkey = lineitem.l_partkey
ORDER BY
  4 DESC,
  5
LIMIT 5
