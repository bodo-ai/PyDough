SELECT
  (
    lineitem.l_extendedprice * (
      1 - (
        POWER(lineitem.l_discount, 2)
      )
    ) + 1.0
  ) / part.p_retailprice AS computed_value,
  lineitem.l_quantity + lineitem.l_extendedprice AS total,
  lineitem.l_extendedprice - lineitem.l_quantity AS delta,
  lineitem.l_quantity * lineitem.l_discount AS product,
  lineitem.l_extendedprice / lineitem.l_quantity AS ratio,
  POWER(lineitem.l_discount, 2) AS exponent
FROM tpch.lineitem AS lineitem
JOIN tpch.part AS part
  ON lineitem.l_partkey = part.p_partkey
