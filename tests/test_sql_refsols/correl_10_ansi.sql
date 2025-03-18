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
  ANTI JOIN (
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
    ON region_key = key
)
ORDER BY
  ordering_0
