SELECT
  agg_3 AS name,
  COALESCE(agg_0, 0) AS n_prefix_nations
FROM (
  SELECT
    MAX(name) AS agg_3,
    COUNT() AS agg_0,
    key
  FROM (
    SELECT
      key,
      name
    FROM (
      SELECT
        _table_alias_0.name AS name,
        _table_alias_1.name AS name_3,
        key,
        region_name
      FROM (
        SELECT
          r_name AS name,
          r_name AS region_name,
          r_regionkey AS key
        FROM tpch.REGION
      ) AS _table_alias_0
      INNER JOIN (
        SELECT
          n_name AS name,
          n_regionkey AS region_key
        FROM tpch.NATION
      ) AS _table_alias_1
        ON key = region_key
    )
    WHERE
      SUBSTRING(name_3, 1, 1) = SUBSTRING(region_name, 1, 1)
  )
  GROUP BY
    key
)
