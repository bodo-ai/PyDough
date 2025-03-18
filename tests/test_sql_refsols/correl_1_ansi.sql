SELECT
  region_name,
  n_prefix_nations
FROM (
  SELECT
    COALESCE(agg_0, 0) AS n_prefix_nations,
    region_name AS ordering_1,
    region_name
  FROM (
    SELECT
      agg_0,
      region_name
    FROM (
      SELECT
        r_name AS region_name,
        r_regionkey AS key
      FROM tpch.REGION
    ) AS _table_alias_0
    LEFT JOIN (
      SELECT
        COUNT() AS agg_0,
        key
      FROM (
        SELECT
          key
        FROM (
          SELECT
            name AS name_3,
            key,
            region_name
          FROM (
            SELECT
              r_name AS region_name,
              r_regionkey AS key
            FROM tpch.REGION
          )
          INNER JOIN (
            SELECT
              n_name AS name,
              n_regionkey AS region_key
            FROM tpch.NATION
          )
            ON key = region_key
        )
        WHERE
          SUBSTRING(name_3, 1, 1) = SUBSTRING(region_name, 1, 1)
      )
      GROUP BY
        key
    ) AS _table_alias_1
      ON _table_alias_0.key = _table_alias_1.key
  )
)
ORDER BY
  ordering_1
