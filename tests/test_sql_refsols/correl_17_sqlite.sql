SELECT
  fullname
FROM (
  SELECT
    CONCAT_WS('-', LOWER(name_3), lname) AS fullname,
    CONCAT_WS('-', LOWER(name_3), lname) AS ordering_0
  FROM (
    SELECT
      name AS name_3,
      lname
    FROM (
      SELECT
        LOWER(name) AS lname,
        region_key
      FROM (
        SELECT
          n_name AS name,
          n_regionkey AS region_key
        FROM tpch.NATION
      )
    )
    INNER JOIN (
      SELECT
        r_regionkey AS key,
        r_name AS name
      FROM tpch.REGION
    )
      ON region_key = key
  )
)
ORDER BY
  ordering_0
