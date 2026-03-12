SELECT
  r_name AS name,
  0 AS n_prefix_nations
FROM tpch.region
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM tpch.nation
    WHERE
      SUBSTRING(n_name, 1, 1) = SUBSTRING(region.r_name, 1, 1)
      AND n_regionkey = region.r_regionkey
  )
