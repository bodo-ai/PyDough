SELECT
  COUNT(*) AS n
FROM tpch.supplier
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM tpch.nation AS nation
    JOIN tpch.region AS region
      ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AFRICA'
    WHERE
      nation.n_nationkey = supplier.s_nationkey
  )
