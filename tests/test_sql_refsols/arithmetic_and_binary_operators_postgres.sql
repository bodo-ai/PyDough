SELECT
  CAST((
    lineitem.l_extendedprice * (
      1 - (
        lineitem.l_discount ^ 2
      )
    ) + 1.0
  ) AS DOUBLE PRECISION) / part.p_retailprice AS computed_value,
  lineitem.l_quantity + lineitem.l_extendedprice AS total,
  lineitem.l_extendedprice - lineitem.l_quantity AS delta,
  lineitem.l_quantity * lineitem.l_discount AS product,
  CAST(lineitem.l_extendedprice AS DOUBLE PRECISION) / lineitem.l_quantity AS ratio,
  lineitem.l_discount ^ 2 AS exponent
FROM tpch.lineitem AS lineitem
JOIN tpch.part AS part
  ON lineitem.l_partkey = part.p_partkey
