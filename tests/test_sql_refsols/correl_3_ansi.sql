SELECT
  region_name,
  n_nations
FROM (
  SELECT
    COALESCE(agg_0, 0) AS n_nations,
    name AS ordering_1,
    region_name
  FROM (
    SELECT
      agg_0,
      name,
      region_name
    FROM (
      SELECT
        r_name AS name,
        r_name AS region_name,
        r_regionkey AS key
      FROM tpch.REGION
    ) AS _table_alias_3
    LEFT JOIN (
      SELECT
        COUNT() AS agg_0,
        key
      FROM (
        SELECT
          key
        FROM (
          SELECT
            _table_alias_1.key AS key,
            _table_alias_2.key AS key_2,
            region_name
          FROM (
            SELECT
              r_name AS region_name,
              r_regionkey AS key
            FROM tpch.REGION
          ) AS _table_alias_1
          INNER JOIN (
            SELECT
              n_nationkey AS key,
              n_regionkey AS region_key
            FROM tpch.NATION
          ) AS _table_alias_2
            ON _table_alias_1.key = region_key
        ) AS _table_alias_0
        SEMI JOIN (
          SELECT
            nation_key
          FROM (
            SELECT
              c_comment AS comment,
              c_nationkey AS nation_key
            FROM tpch.CUSTOMER
          )
          WHERE
            SUBSTRING(comment, 1, 2) = LOWER(SUBSTRING(_table_alias_0.region_name, 1, 2))
        )
          ON key_2 = nation_key
      )
      GROUP BY
        key
    ) AS _table_alias_4
      ON _table_alias_3.key = _table_alias_4.key
  )
)
ORDER BY
  ordering_1
