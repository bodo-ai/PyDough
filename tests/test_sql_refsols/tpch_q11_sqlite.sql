SELECT
  PS_PARTKEY,
  VALUE
FROM (
  SELECT
    PS_PARTKEY,
    VALUE,
    ordering_2
  FROM (
    SELECT
      VALUE AS ordering_2,
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
                ) AS _table_alias_2
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
                    ) AS _t7
                    WHERE
                      name = 'GERMANY'
                  ) AS _table_alias_1
                    ON nation_key = _table_alias_1.key
                ) AS _table_alias_3
                  ON supplier_key = key
              ) AS _t6
            ) AS _t5
          ) AS _t4
        ) AS _table_alias_8
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
              ) AS _table_alias_6
              INNER JOIN (
                SELECT
                  _table_alias_4.key AS key
                FROM (
                  SELECT
                    s_suppkey AS key,
                    s_nationkey AS nation_key
                  FROM tpch.SUPPLIER
                ) AS _table_alias_4
                INNER JOIN (
                  SELECT
                    key
                  FROM (
                    SELECT
                      n_name AS name,
                      n_nationkey AS key
                    FROM tpch.NATION
                  ) AS _t10
                  WHERE
                    name = 'GERMANY'
                ) AS _table_alias_5
                  ON nation_key = _table_alias_5.key
              ) AS _table_alias_7
                ON supplier_key = key
            ) AS _t9
          ) AS _t8
          GROUP BY
            part_key
        ) AS _table_alias_9
          ON TRUE
      ) AS _t3
    ) AS _t2
    WHERE
      VALUE > min_market_share
  ) AS _t1
  ORDER BY
    ordering_2 DESC
  LIMIT 10
) AS _t0
ORDER BY
  ordering_2 DESC
