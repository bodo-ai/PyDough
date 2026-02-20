SELECT
  PART.p_name AS name,
  COLUMN1 AS shipping_type,
  LINEITEM.l_extendedprice AS extended_price,
  LINEITEM.l_discount + COLUMN2 AS added_discount,
  LINEITEM.l_extendedprice * (
    1 - (
      LINEITEM.l_discount + COLUMN2
    )
  ) AS final_price
FROM (VALUES
  ('REG AIR', 0.05),
  ('SHIP', 0.06),
  ('TRUCK', 0.05)) AS DISCOUNTS(SHIPPING_TYPE, ADDED_DISCOUNT)
JOIN TPCH.LINEITEM LINEITEM
  ON COLUMN1 = LINEITEM.l_shipmode
JOIN TPCH.PART PART
  ON LINEITEM.l_partkey = PART.p_partkey
ORDER BY
  5 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
