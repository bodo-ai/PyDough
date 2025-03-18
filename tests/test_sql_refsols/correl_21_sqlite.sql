SELECT
  COUNT() AS n_sizes
FROM (
  SELECT
    agg_0
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS n_parts,
      agg_0,
      avg_n_parts
    FROM (
      SELECT
        agg_0,
        avg_n_parts
      FROM (
        SELECT
          AVG(n_parts) AS avg_n_parts
        FROM (
          SELECT
            COALESCE(agg_0, 0) AS n_parts
          FROM (
            SELECT
              COUNT() AS agg_0,
              size
            FROM (
              SELECT
                p_size AS size
              FROM tpch.PART
            )
            GROUP BY
              size
          )
        )
      )
      LEFT JOIN (
        SELECT
          COUNT() AS agg_0,
          size
        FROM (
          SELECT
            p_size AS size
          FROM tpch.PART
        )
        GROUP BY
          size
      )
        ON TRUE
    )
  )
  WHERE
    n_parts > avg_n_parts
)
