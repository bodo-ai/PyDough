SELECT
  container,
  n_types
FROM (
  SELECT
    container,
    n_types,
    ordering_2,
    ordering_3
  FROM (
    SELECT
      COALESCE(agg_1, 0) AS n_types,
      COALESCE(agg_1, 0) AS ordering_2,
      container AS ordering_3,
      container
    FROM (
      SELECT
        COUNT() AS agg_1,
        container
      FROM (
        SELECT
          container
        FROM (
          SELECT
            agg_0,
            container,
            global_avg_price
          FROM (
            SELECT
              AVG(retail_price) AS global_avg_price
            FROM (
              SELECT
                p_retailprice AS retail_price
              FROM tpch.PART
            )
          )
          LEFT JOIN (
            SELECT
              AVG(retail_price) AS agg_0,
              container,
              part_type
            FROM (
              SELECT
                p_container AS container,
                p_retailprice AS retail_price,
                p_type AS part_type
              FROM tpch.PART
            )
            GROUP BY
              part_type,
              container
          )
            ON TRUE
        )
        WHERE
          agg_0 > global_avg_price
      )
      GROUP BY
        container
    )
  )
  ORDER BY
    ordering_2 DESC,
    ordering_3
  LIMIT 5
)
ORDER BY
  ordering_2 DESC,
  ordering_3
