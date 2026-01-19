SELECT
  part.p_name AS name,
  discounts.column1 AS shipping_type,
  lineitem.l_extendedprice AS extended_price,
  lineitem.l_discount + discounts.column2 AS added_discount,
  lineitem.l_extendedprice * (
    1 - (
      lineitem.l_discount + discounts.column2
    )
  ) AS final_price
FROM (VALUES
  ('REG AIR', 0.05),
  ('SHIP', 0.06),
  ('TRUCK', 0.05)) AS discounts
JOIN tpch.lineitem AS lineitem
  ON discounts.column1 = lineitem.l_shipmode
JOIN tpch.part AS part
  ON lineitem.l_partkey = part.p_partkey
ORDER BY
  lineitem.l_extendedprice * (
    1 - (
      lineitem.l_discount + added_discount
    )
  )
LIMIT 5
