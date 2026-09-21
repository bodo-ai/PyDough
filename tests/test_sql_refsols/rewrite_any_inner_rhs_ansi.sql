WITH _s1 AS (
  SELECT
    o_custkey
  FROM tpch.orders
  WHERE
    o_clerk = 'Clerk#000000470' AND o_totalprice = 252004.18
)
SELECT
  ANY_VALUE(customer.c_name) AS any_customer
FROM tpch.customer AS customer
SEMI JOIN _s1 AS _s1
  ON _s1.o_custkey = customer.c_custkey
