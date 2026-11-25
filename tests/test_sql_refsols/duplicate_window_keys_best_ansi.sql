WITH _t2 AS (
  SELECT
    o_custkey,
    o_orderkey
  FROM tpch.orders
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY o_custkey
      ORDER BY o_totalprice DESC NULLS FIRST, o_orderkey NULLS LAST
    ) = 1
)
SELECT
  customer.c_custkey AS key,
  _t2.o_orderkey AS best_order
FROM tpch.customer AS customer
LEFT JOIN _t2 AS _t2
  ON _t2.o_custkey = customer.c_custkey
WHERE
  customer.c_custkey <= 3
ORDER BY
  1
