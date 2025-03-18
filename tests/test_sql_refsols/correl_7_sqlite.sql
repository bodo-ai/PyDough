SELECT
  name,
  COALESCE(NULL_2, 0) AS n_prefix_nations
FROM (
  SELECT
    NULL AS NULL_2,
    name
  FROM (
    SELECT
      r_name AS name,
      r_name AS region_name,
      r_regionkey AS key
    FROM tpch.REGION
  ) AS _table_alias_0
  WHERE
    NOT EXISTS(
      SELECT
        1
      FROM (
        SELECT
          region_key
        FROM (
          SELECT
            n_name AS name,
            n_regionkey AS region_key
          FROM tpch.NATION
        )
        WHERE
          SUBSTRING(name, 1, 1) = SUBSTRING(_table_alias_0.region_name, 1, 1)
      )
      WHERE
        key = region_key
    )
)
