WITH "_S0" AS (
  SELECT
    p_partkey AS P_PARTKEY,
    p_retailprice AS P_RETAILPRICE,
    p_size AS P_SIZE
  FROM TPCH.PART
  ORDER BY
    TRUNC(CAST(p_retailprice AS DOUBLE PRECISION), '0') NULLS FIRST
  FETCH FIRST 2 ROWS ONLY
)
SELECT
  CAST("_S0".P_SIZE / 2.5 AS DOUBLE PRECISION) AS reduced_size,
  TRUNC(CAST("_S0".P_RETAILPRICE AS DOUBLE PRECISION), '0') AS retail_price_int,
  'old size: ' || NVL(TO_CHAR("_S0".P_SIZE), '') AS message,
  LINEITEM.l_discount AS discount,
  TO_CHAR(LINEITEM.l_receiptdate, 'DD-MM-YYYY') AS date_dmy,
  TO_CHAR(LINEITEM.l_receiptdate, 'MM/DD') AS date_md,
  TO_CHAR(LINEITEM.l_receiptdate, 'HH24:MIAM') AS am_pm
FROM "_S0" "_S0"
JOIN TPCH.LINEITEM LINEITEM
  ON LINEITEM.l_partkey = "_S0".P_PARTKEY
ORDER BY
  4 DESC NULLS LAST,
  5 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
