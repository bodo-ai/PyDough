WITH "_T" AS (
  SELECT
    LINEITEM.l_shipdate AS L_SHIPDATE,
    PART.p_container AS P_CONTAINER,
    ROW_NUMBER() OVER (PARTITION BY PART.p_container ORDER BY LINEITEM.l_extendedprice DESC, LINEITEM.l_shipdate) AS "_W"
  FROM TPCH.PART PART
  JOIN TPCH.LINEITEM LINEITEM
    ON LINEITEM.l_partkey = PART.p_partkey
    AND LINEITEM.l_shipmode = 'RAIL'
    AND LINEITEM.l_tax = 0
  WHERE
    PART.p_brand = 'Brand#13'
)
SELECT
  PART.p_container AS container,
  ANY_VALUE("_T".L_SHIPDATE) AS highest_price_ship_date
FROM TPCH.PART PART
JOIN "_T" "_T"
  ON PART.p_container = "_T".P_CONTAINER AND "_T"."_W" = 1
WHERE
  PART.p_brand = 'Brand#13'
GROUP BY
  PART.p_container
ORDER BY
  2 NULLS FIRST,
  1 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
