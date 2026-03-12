SELECT
  n_name AS name,
  NULL AS rname
FROM tpch.nation
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM tpch.region
    WHERE
      SUBSTRING(nation.n_name, 1, 1) = SUBSTRING(r_name, 1, 1)
      AND nation.n_regionkey = r_regionkey
  )
ORDER BY
  1
