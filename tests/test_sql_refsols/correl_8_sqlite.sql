SELECT
  name,
  rname
FROM (
  SELECT
    name AS ordering_0,
    name_3 AS rname,
    name
  FROM (
    SELECT
      n_nationkey AS key,
      n_name AS name
    FROM tpch.NATION
  ) AS _table_alias_2
  LEFT JOIN (
    SELECT
      key,
      name_3
    FROM (
      SELECT
        _table_alias_0.key AS key,
        name AS name_3,
        nation_name
      FROM (
        SELECT
          n_name AS nation_name,
          n_nationkey AS key,
          n_regionkey AS region_key
        FROM tpch.NATION
      ) AS _table_alias_0
      INNER JOIN (
        SELECT
          r_regionkey AS key,
          r_name AS name
        FROM tpch.REGION
      ) AS _table_alias_1
        ON region_key = _table_alias_1.key
    )
    WHERE
      SUBSTRING(name_3, 1, 1) = SUBSTRING(nation_name, 1, 1)
  ) AS _table_alias_3
    ON _table_alias_2.key = _table_alias_3.key
)
ORDER BY
  ordering_0
