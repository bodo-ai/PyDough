SELECT
  PART.p_name AS name,
  discounts.shipping_type,
  LINEITEM.l_extendedprice AS extended_price,
  LINEITEM.l_discount + discounts.added_discount AS added_discount,
  LINEITEM.l_extendedprice * (
    1 - (
      LINEITEM.l_discount + discounts.added_discount
    )
  ) AS final_price
FROM (VALUES
  ROW('REG AIR', 0.05),
  ROW('SHIP', 0.06),
  ROW('TRUCK', 0.05)) AS discounts(shipping_type, added_discount)
JOIN tpch.LINEITEM AS LINEITEM
  ON LINEITEM.l_shipmode = discounts.shipping_type
JOIN tpch.PART AS PART
  ON LINEITEM.l_partkey = PART.p_partkey
ORDER BY
  LINEITEM.l_extendedprice * (
    1 - (
      LINEITEM.l_discount + added_discount
    )
  )
LIMIT 5
