WITH _s0 AS (
  SELECT
    p_partkey,
    p_retailprice,
    p_size
  FROM tpch.part
  ORDER BY
    CAST(p_retailprice AS BIGINT) NULLS FIRST
  LIMIT 2
)
SELECT
  CAST(_s0.p_size / 2.5 AS DOUBLE) AS reduced_size,
  CAST(_s0.p_retailprice AS BIGINT) AS retail_price_int,
  CONCAT_WS('', 'old size: ', CAST(_s0.p_size AS TEXT)) AS message,
  lineitem.l_discount AS discount,
  TO_CHAR(CAST(lineitem.l_receiptdate AS TIMESTAMP), 'DD-mm-yyyy') AS date_dmy,
  TO_CHAR(CAST(lineitem.l_receiptdate AS TIMESTAMP), 'mm/DD') AS date_md,
  TO_CHAR(CAST(lineitem.l_receiptdate AS TIMESTAMP), 'hh24:mi%p') AS am_pm
FROM _s0 AS _s0
JOIN tpch.lineitem AS lineitem
  ON _s0.p_partkey = lineitem.l_partkey
ORDER BY
  4 DESC NULLS LAST
LIMIT 5
