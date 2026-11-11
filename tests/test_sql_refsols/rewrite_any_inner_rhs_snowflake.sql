WITH _u_0 AS (
  SELECT
    o_custkey AS _u_1
  FROM snowflake_sample_data.tpch_sf1.orders
  WHERE
    o_clerk = 'Clerk#000000470' AND o_totalprice = 252004.18
  GROUP BY
    1
)
SELECT
  ANY_VALUE(customer.c_name) AS any_customer
FROM snowflake_sample_data.tpch_sf1.customer AS customer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = customer.c_custkey
WHERE
  NOT _u_0._u_1 IS NULL
