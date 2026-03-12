SELECT
  COUNT(*) AS n
FROM tpch.SUPPLIER
WHERE
  NOT EXISTS(
    SELECT
      1 AS `1`
    FROM tpch.NATION AS NATION
    JOIN tpch.REGION AS REGION
      ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'AFRICA'
    WHERE
      NATION.n_nationkey = SUPPLIER.s_nationkey
  )
