SELECT
  (
    LINEITEM.l_extendedprice * (
      1 - (
        POWER(LINEITEM.l_discount, 2)
      )
    ) + 1.0
  ) / PART.p_retailprice AS computed_value,
  LINEITEM.l_quantity + LINEITEM.l_extendedprice AS total,
  LINEITEM.l_extendedprice - LINEITEM.l_quantity AS delta,
  LINEITEM.l_quantity * LINEITEM.l_discount AS product,
  LINEITEM.l_extendedprice / LINEITEM.l_quantity AS ratio,
  POWER(LINEITEM.l_discount, 2) AS exponent
FROM tpch.LINEITEM AS LINEITEM
JOIN tpch.PART AS PART
  ON LINEITEM.l_partkey = PART.p_partkey
