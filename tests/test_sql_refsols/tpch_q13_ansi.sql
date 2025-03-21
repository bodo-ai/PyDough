SELECT
  C_COUNT,
  CUSTDIST
FROM (
  SELECT
    CUSTDIST,
    C_COUNT,
    ordering_1,
    ordering_2
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS CUSTDIST,
      COALESCE(agg_0, 0) AS ordering_1,
      num_non_special_orders AS C_COUNT,
      num_non_special_orders AS ordering_2
    FROM (
      SELECT
        COUNT() AS agg_0,
        num_non_special_orders
      FROM (
        SELECT
          COALESCE(agg_0, 0) AS num_non_special_orders
        FROM (
          SELECT
            agg_0
          FROM (
            SELECT
              c_custkey AS key
            FROM tpch.CUSTOMER
          )
          LEFT JOIN (
            SELECT
              COUNT() AS agg_0,
              customer_key
            FROM (
              SELECT
                customer_key
              FROM (
                SELECT
                  o_comment AS comment,
                  o_custkey AS customer_key
                FROM tpch.ORDERS
              )
              WHERE
                NOT comment LIKE '%special%requests%'
            )
            GROUP BY
              customer_key
          )
            ON key = customer_key
        )
      )
      GROUP BY
        num_non_special_orders
    )
  )
  ORDER BY
    ordering_1 DESC,
    ordering_2 DESC
  LIMIT 10
)
ORDER BY
  ordering_1 DESC,
  ordering_2 DESC
