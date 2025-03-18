SELECT
  name_12 AS name,
  n_selected_custs
FROM (
  SELECT
    COALESCE(agg_0, 0) AS n_selected_custs,
    name_3 AS name_12,
    name_3 AS ordering_1
  FROM (
    SELECT
      agg_0,
      name_3
    FROM (
      SELECT
        _table_alias_0.key AS key,
        _table_alias_1.key AS key_2,
        name AS name_3
      FROM (
        SELECT
          key
        FROM (
          SELECT
            r_name AS name,
            r_regionkey AS key
          FROM tpch.REGION
        )
        WHERE
          NOT name LIKE 'A%'
      ) AS _table_alias_0
      INNER JOIN (
        SELECT
          n_nationkey AS key,
          n_name AS name,
          n_regionkey AS region_key
        FROM tpch.NATION
      ) AS _table_alias_1
        ON _table_alias_0.key = region_key
    ) AS _table_alias_4
    LEFT JOIN (
      SELECT
        COUNT() AS agg_0,
        key,
        key_5
      FROM (
        SELECT
          key,
          key_5
        FROM (
          SELECT
            comment AS comment_7,
            key,
            key_5,
            region_name
          FROM (
            SELECT
              _table_alias_2.key AS key,
              _table_alias_3.key AS key_5,
              region_name
            FROM (
              SELECT
                name AS region_name,
                key
              FROM (
                SELECT
                  r_name AS name,
                  r_regionkey AS key
                FROM tpch.REGION
              )
              WHERE
                NOT name LIKE 'A%'
            ) AS _table_alias_2
            INNER JOIN (
              SELECT
                n_nationkey AS key,
                n_regionkey AS region_key
              FROM tpch.NATION
            ) AS _table_alias_3
              ON _table_alias_2.key = region_key
          )
          INNER JOIN (
            SELECT
              c_comment AS comment,
              c_nationkey AS nation_key
            FROM tpch.CUSTOMER
          )
            ON key_5 = nation_key
        )
        WHERE
          SUBSTRING(comment_7, 1, 1) = LOWER(SUBSTRING(region_name, 1, 1))
      )
      GROUP BY
        key_5,
        key
    ) AS _table_alias_5
      ON (
        key_2 = key_5
      ) AND (
        _table_alias_4.key = _table_alias_5.key
      )
  )
)
ORDER BY
  ordering_1
