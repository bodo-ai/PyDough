SELECT
  PS_PARTKEY,
  VALUE
FROM (
  SELECT
    COALESCE(agg_1, 0) AS VALUE,
    part_key AS PS_PARTKEY,
    min_market_share
  FROM (
    SELECT
      agg_1,
      min_market_share,
      part_key
    FROM (
      SELECT
        COALESCE(agg_0, 0) * 0.0001 AS min_market_share
      FROM (
        SELECT
          SUM(metric) AS agg_0
        FROM (
          SELECT
            supplycost * availqty AS metric
          FROM (
            SELECT
              availqty,
              supplycost
            FROM (
              SELECT
                ps_availqty AS availqty,
                ps_suppkey AS supplier_key,
                ps_supplycost AS supplycost
              FROM tpch.PARTSUPP
            )
            INNER JOIN (
              SELECT
                _table_alias_0.key AS key
              FROM (
                SELECT
                  s_suppkey AS key,
                  s_nationkey AS nation_key
                FROM tpch.SUPPLIER
              ) AS _table_alias_0
              INNER JOIN (
                SELECT
                  key
                FROM (
                  SELECT
                    n_name AS name,
                    n_nationkey AS key
                  FROM tpch.NATION
                )
                WHERE
                  name = 'GERMANY'
              ) AS _table_alias_1
                ON nation_key = _table_alias_1.key
            )
              ON supplier_key = key
          )
        )
      )
    )
    LEFT JOIN (
      SELECT
        SUM(metric) AS agg_1,
        part_key
      FROM (
        SELECT
          supplycost * availqty AS metric,
          part_key
        FROM (
          SELECT
            availqty,
            part_key,
            supplycost
          FROM (
            SELECT
              ps_availqty AS availqty,
              ps_partkey AS part_key,
              ps_suppkey AS supplier_key,
              ps_supplycost AS supplycost
            FROM tpch.PARTSUPP
          )
          INNER JOIN (
            SELECT
              _table_alias_2.key AS key
            FROM (
              SELECT
                s_suppkey AS key,
                s_nationkey AS nation_key
              FROM tpch.SUPPLIER
            ) AS _table_alias_2
            INNER JOIN (
              SELECT
                key
              FROM (
                SELECT
                  n_name AS name,
                  n_nationkey AS key
                FROM tpch.NATION
              )
              WHERE
                name = 'GERMANY'
            ) AS _table_alias_3
              ON nation_key = _table_alias_3.key
          )
            ON supplier_key = key
        )
      )
      GROUP BY
        part_key
    )
      ON TRUE
  )
)
WHERE
  VALUE > min_market_share
ORDER BY
  VALUE DESC
LIMIT 10
