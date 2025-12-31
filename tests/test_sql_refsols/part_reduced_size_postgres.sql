WITH _s0 AS (
  SELECT
    p_partkey,
    p_retailprice,
    p_size
  FROM tpch.part
  ORDER BY
    TRUNC(CAST(p_retailprice AS DECIMAL)) NULLS FIRST
  LIMIT 2
)
SELECT
  CAST(_s0.p_size AS DOUBLE PRECISION) / 2.5 AS reduced_size,
  TRUNC(CAST(_s0.p_retailprice AS DECIMAL)) AS retail_price_int,
  CONCAT_WS('', 'old size: ', CAST(_s0.p_size AS TEXT)) AS message,
  lineitem.l_discount AS discount,
  TO_CHAR(lineitem.l_receiptdate, 'DD-MM-YYYY') AS date_dmy,
  TO_CHAR(lineitem.l_receiptdate, 'MM/DD') AS date_md,
  TO_CHAR(lineitem.l_receiptdate, 'HH24:MIPM') AS am_pm
FROM _s0 AS _s0
JOIN tpch.lineitem AS lineitem
  ON _s0.p_partkey = lineitem.l_partkey
ORDER BY
  4 DESC NULLS LAST
LIMIT 5
