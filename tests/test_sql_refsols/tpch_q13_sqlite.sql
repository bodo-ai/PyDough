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
          ) AS _table_alias_0
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
              ) AS _t6
              WHERE
                NOT comment LIKE '%special%requests%'
            ) AS _t5
            GROUP BY
              customer_key
          ) AS _table_alias_1
            ON key = customer_key
        ) AS _t4
      ) AS _t3
      GROUP BY
        num_non_special_orders
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_1 DESC,
    ordering_2 DESC
  LIMIT 10
) AS _t0
ORDER BY
  ordering_1 DESC,
  ordering_2 DESC
