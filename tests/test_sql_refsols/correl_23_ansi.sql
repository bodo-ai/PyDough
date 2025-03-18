SELECT
  COUNT() AS n_sizes
FROM (
  SELECT
    agg_0
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS n_combos,
      agg_0,
      avg_n_combo
    FROM (
      SELECT
        agg_0,
        avg_n_combo
      FROM (
        SELECT
          AVG(n_combos) AS avg_n_combo
        FROM (
          SELECT
            COALESCE(agg_0, 0) AS n_combos
          FROM (
            SELECT
              COUNT() AS agg_0,
              size
            FROM (
              SELECT DISTINCT
                container,
                part_type,
                size
              FROM (
                SELECT
                  p_container AS container,
                  p_size AS size,
                  p_type AS part_type
                FROM tpch.PART
              )
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
          SELECT DISTINCT
            container,
            part_type,
            size
          FROM (
            SELECT
              p_container AS container,
              p_size AS size,
              p_type AS part_type
            FROM tpch.PART
          )
        )
        GROUP BY
          size
      )
        ON TRUE
    )
  )
  WHERE
    n_combos > avg_n_combo
)
