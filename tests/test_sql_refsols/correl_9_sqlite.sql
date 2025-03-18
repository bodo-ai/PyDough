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
      _table_alias_0.name AS name,
      _table_alias_1.name AS name_3,
      nation_name
    FROM (
      SELECT
        n_name AS name,
        n_name AS nation_name,
        n_regionkey AS region_key
      FROM tpch.NATION
    ) AS _table_alias_0
    INNER JOIN (
      SELECT
        r_regionkey AS key,
        r_name AS name
      FROM tpch.REGION
    ) AS _table_alias_1
      ON region_key = key
  )
  WHERE
    SUBSTRING(name_3, 1, 1) = SUBSTRING(nation_name, 1, 1)
)
ORDER BY
  ordering_0
