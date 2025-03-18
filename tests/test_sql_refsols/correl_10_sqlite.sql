SELECT
  name,
  rname
FROM (
  SELECT
    name AS ordering_0,
    NULL AS rname,
    name
  FROM (
    SELECT
      n_name AS name,
      n_name AS nation_name,
      n_regionkey AS region_key
    FROM tpch.NATION
  ) AS _table_alias_0
  WHERE
    NOT EXISTS(
      SELECT
        1
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
          SUBSTRING(name, 1, 1) = SUBSTRING(_table_alias_0.nation_name, 1, 1)
      )
      WHERE
        region_key = key
    )
)
ORDER BY
  ordering_0
