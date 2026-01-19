SELECT
  part.p_name AS name,
  discounts.shipping_type,
  lineitem.l_extendedprice AS extended_price,
  lineitem.l_discount + discounts.added_discount AS added_discount,
  lineitem.l_extendedprice * (
    1 - (
      lineitem.l_discount + discounts.added_discount
    )
  ) AS final_price
FROM (VALUES
  ('REG AIR', 0.05),
  ('SHIP', 0.06),
  ('TRUCK', 0.05)) AS discounts(shipping_type, added_discount)
JOIN tpch.lineitem AS lineitem
  ON discounts.shipping_type = lineitem.l_shipmode
JOIN tpch.part AS part
  ON lineitem.l_partkey = part.p_partkey
ORDER BY
  lineitem.l_extendedprice * (
    1 - (
      lineitem.l_discount + added_discount
    )
  ) NULLS FIRST
LIMIT 5
